/*
 *  Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.isValid
import com.exactpro.th2.common.utils.event.logId
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

abstract class AbstractCodecProcessor<BATCH, GROUP, MESSAGE>(
    protected val codec: IPipelineCodec,
    private val protocols: Set<String>,
    private val codecEventID: EventID,
    private val useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    private val process: Process,
    private val onEvent: (event: ProtoEvent) -> Unit
) {
    private val async = enabledVerticalScaling && Runtime.getRuntime().availableProcessors() > 1

    protected abstract val BATCH.batchItems: List<GROUP>
    protected abstract val GROUP.groupItems: List<MESSAGE>
    protected abstract val GROUP.size: Int
    protected abstract val GROUP.rawProtocols: Set<String>
    protected abstract val GROUP.parsedProtocols: Set<String>
    protected abstract val GROUP.eventIds: Sequence<EventID>
    protected abstract fun GROUP.ids(batch: BATCH): List<MessageID>
    protected abstract fun GROUP.toReadableBody(): List<String>
    protected abstract val MESSAGE.isRaw: Boolean
    protected abstract val MESSAGE.isParsed: Boolean
    protected abstract fun createBatch(sourceBatch: BATCH, groups: List<GROUP>): BATCH
    protected abstract fun IPipelineCodec.genericDecode(group: GROUP, context: ReportingContext): GROUP
    protected abstract fun IPipelineCodec.genericEncode(group: GROUP, context: ReportingContext): GROUP

    enum class Process(
        val inputMessageType: String,
        val isInvalidResultSize: (inputGroupSize: Int, outputGroupSize: Int) -> Boolean,
        val operationName: String
    ) {
        DECODE(
            inputMessageType = "raw",
            isInvalidResultSize = { inputGroupSize, outputGroupSize -> inputGroupSize > outputGroupSize },
            operationName = "less"
        ),
        ENCODE(
            inputMessageType = "parsed",
            isInvalidResultSize = { inputGroupSize, outputGroupSize -> inputGroupSize < outputGroupSize },
            operationName = "more"
        );

        val actionName = name.lowercase()
        val opposite: Process get() = when(this) {
            DECODE -> ENCODE
            ENCODE -> DECODE
        }
    }

    private val GROUP.protocols: Set<String> get() = when(process) {
        Process.DECODE -> rawProtocols
        Process.ENCODE -> parsedProtocols
    }

    private fun IPipelineCodec.recode(group: GROUP, context: ReportingContext): GROUP = when(process) {
        Process.DECODE -> genericDecode(group, context)
        Process.ENCODE -> genericEncode(group, context)
    }

    private val MESSAGE.isInputMessage: Boolean get() = when(process) {
        Process.DECODE -> isRaw
        Process.ENCODE -> isParsed
    }

    protected abstract val toErrorGroup: GROUP.(infoMessage: String, protocols: Collection<String>, throwable: Throwable, errorEventID: EventID) -> GROUP

    protected fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()) = codecEventID.onEvent(message, messagesIds)

    private fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = codecEventID.onErrorEvent(message, messagesIds, cause)

    fun process(source: BATCH): BATCH {
        val sourceGroups = source.batchItems
        val resultGroups = mutableListOf<GROUP>()

        if (async) {
            val messageGroupFutures = Array(sourceGroups.size) {
                processMessageGroupAsync(source, sourceGroups[it])
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultGroups::add) }
            }.get()
        } else {
            sourceGroups.forEach { group ->
                processMessageGroup(source, group)?.run(resultGroups::add)
            }
        }

        if (sourceGroups.size != resultGroups.size) {
            onErrorEvent("Group count in the processed batch (${resultGroups.size}) is different from the input one (${sourceGroups.size})")
        }

        return createBatch(source, resultGroups)
    }

    private fun processMessageGroupAsync(batch: BATCH, group: GROUP) = CompletableFuture.supplyAsync {
        processMessageGroup(batch, group)
    }

    private fun processMessageGroup(batch: BATCH, messageGroup: GROUP): GROUP? {
        if (messageGroup.size == 0) {
            onErrorEvent("Cannot ${process.actionName} empty message group")
            return null
        }
/*
        if (messageGroup.groupItems.none { it.isInputMessage }) {
            LOGGER.debug { "Message group has no ${process.inputMessageType} messages in it" }
            return messageGroup
        }
*/
        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.eventIds else emptySequence()
        val context = ReportingContext()

        try {
            val msgProtocols = messageGroup.protocols
 /*
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                LOGGER.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }
*/
            val recodedGroup = codec.recode(messageGroup, context)

            if (process.isInvalidResultSize(messageGroup.size, recodedGroup.size)) {
                parentEventIds.onEachEvent("${process.name}d message group contains ${process.operationName} messages (${recodedGroup.size}) than ${process.opposite.actionName}d one (${messageGroup.size})")
            }

            return recodedGroup
        } catch (e: ValidateException) {
            val header = "Failed to ${process.actionName}: ${e.title}"

            return when (process) {
                Process.DECODE -> {
                    val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.ids(batch), e)
                    messageGroup.toErrorGroup(header, protocols, e, errorEventId)
                }
                Process.ENCODE -> {
                    parentEventIds.onEachErrorEvent(header, messageGroup.ids(batch), e, e.details + messageGroup.toReadableBody())
                    null
                }
            }
        } catch (throwable: Throwable) {
            val header = "Failed to ${process.actionName} message group"
            return when (process) {
                Process.DECODE -> {
                    val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.ids(batch), throwable)
                    messageGroup.toErrorGroup(header, protocols, throwable, errorEventId)
                }
                Process.ENCODE -> {
                    // we should not use message IDs because during encoding there is no correct message ID created yet
                    parentEventIds.onEachErrorEvent(header, messageGroup.ids(batch), throwable, messageGroup.toReadableBody())
                    null
                }
            }
        } finally {
            when (process) {
                Process.DECODE -> parentEventIds.onEachWarning(context, "decoding") { messageGroup.ids(batch) }
                Process.ENCODE -> parentEventIds.onEachWarning(context, "encoding", additionalBody = { messageGroup.toReadableBody() })
            }
        }
    }

    private fun EventID.onEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList(),
    ) : EventID {
        LOGGER.warn { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        return this.publishEvent(message, messagesIds, body = body).id
    }

    private fun EventID.onErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): EventID {
        LOGGER.error(cause) { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        return publishEvent(message, messagesIds, FAILED, cause, additionalBody).id
    }

    private fun Sequence<EventID>.onEachEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = codecEventID.onEvent(message, messagesIds, body)
        forEach {
            it.addReferenceTo(warnEvent, message, PASSED)
        }
    }

    private fun Sequence<EventID>.onEachErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList(),
    ): EventID {
        val errorEventId = codecEventID.onErrorEvent(message, messagesIds, cause, additionalBody)
        forEach { it.addReferenceTo(errorEventId, message, FAILED) }
        return errorEventId
    }

    private fun Sequence<EventID>.onEachWarning(
        context: ReportingContext,
        action: String,
        additionalBody: () -> List<String> = ::emptyList,
        messagesIds: () -> List<MessageID> = ::emptyList
    ) = context.warnings.let { warnings ->
        if (warnings.isNotEmpty()) {
            val messages = messagesIds()
            val body = additionalBody()
            warnings.forEach { warning ->
                this.onEachEvent("[WARNING] During $action: $warning", messages, body)
            }
        }
    }

    private fun List<MessageID>.joinToReadableString(): String =
        joinToString(", ") {
            "${it.connectionId.sessionAlias}:${it.direction}:${it.sequence}[.${it.subsequenceList.joinToString(".")}]"
        }

    private fun EventID.addReferenceTo(eventId: EventID, name: String, status: Status): EventID = Event.start()
        .endTimestamp()
        .name(name)
        .status(status)
        .type(if (status != PASSED) "Error" else "Warn")
        .bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
        .bodyData(ReferenceToEvent(eventId.logId))
        .toProto(this)
        .also(onEvent)
        .id

    private fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
        incomingProtocols.none { it.isBlank() || it in this }  -> false
        incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
        else -> true
    }

    private fun EventID.publishEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Status = PASSED,
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ): ProtoEvent = Event.start().apply {
        name(message)
        type(if (status != PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) FAILED else status)
        messagesIds.forEach { messageId ->
            if (messageId.isValid) {
                messageID(messageId)
            }
        }

        generateSequence(cause, Throwable::cause).forEach {
            bodyData(EventUtils.createMessageBean(it.message))
        }

        if (body.isNotEmpty()) {
            bodyData(EventUtils.createMessageBean("Information:"))
            body.forEach { bodyData(EventUtils.createMessageBean(it)) }
        }
    }.toProto(this).also(onEvent)

    @Suppress("unused")
    private class ReferenceToEvent(val eventId: String) : IBodyData {
        val type: String
            get() = TYPE

        companion object {
            const val TYPE = "reference"
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}