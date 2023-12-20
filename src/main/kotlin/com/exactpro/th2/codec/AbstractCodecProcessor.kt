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
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

abstract class AbstractCodecProcessor<BATCH, GROUP, MESSAGE>(
    protected val codec: IPipelineCodec,
    private val protocols: Set<String>,
    private val useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    private val process: Process,
    private val eventProcessor: EventProcessor,
    private val config: Configuration
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
    protected abstract fun MESSAGE.id(batch: BATCH): MessageID
    protected abstract fun MESSAGE.book(batch: BATCH): String
    protected abstract val MESSAGE.eventBook: String?
    protected abstract val MESSAGE.eventId: EventID?
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

    protected abstract val toErrorGroup: GROUP.(
        batch: BATCH,
        infoMessage: String,
        protocols: Collection<String>,
        throwable: Throwable,
        bookToEventId: Map<String, EventID>
    ) -> GROUP

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
            eventProcessor.onErrorEvent(message = "Group count in the processed batch (${resultGroups.size}) is different from the input one (${sourceGroups.size})")
        }

        return createBatch(source, resultGroups)
    }

    private fun processMessageGroupAsync(batch: BATCH, group: GROUP) = CompletableFuture.supplyAsync {
        processMessageGroup(batch, group)
    }

    private fun processMessageGroup(batch: BATCH, messageGroup: GROUP): GROUP? {
        if (messageGroup.size == 0) {
            eventProcessor.onErrorEvent(message = "Cannot ${process.actionName} empty message group")
            return null
        }

        if (!config.disableMessageTypeCheck) {
            if (messageGroup.groupItems.none { it.isInputMessage }) {
                LOGGER.debug { "Message group has no ${process.inputMessageType} messages in it" }
                return messageGroup
            }
        }

        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.eventIds else emptySequence()
        val context = ReportingContext()

        try {
            if (!config.disableProtocolCheck) {
                val msgProtocols = messageGroup.protocols
                if (!protocols.checkAgainstProtocols(msgProtocols)) {
                    LOGGER.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                    return messageGroup
                }
            }

            val recodedGroup = codec.recode(messageGroup, context)

            if (process.isInvalidResultSize(messageGroup.size, recodedGroup.size)) {
                eventProcessor.onEachEvent(parentEventIds.toList(), "${process.name}d message group contains ${process.operationName} messages (${recodedGroup.size}) than ${process.opposite.actionName}d one (${messageGroup.size})")
            }

            return recodedGroup
        } catch (e: ValidateException) {
            val header = "Failed to ${process.actionName}: ${e.title}"

            return when (process) {
                Process.DECODE -> {
                    val errorEventIds: Map<String, EventID> = eventProcessor.onEachErrorEvent(parentEventIds.toList(), header, messageGroup.ids(batch), e)
                    messageGroup.toErrorGroup(batch, header, protocols, e, errorEventIds)
                }
                Process.ENCODE -> {
                    eventProcessor.onEachErrorEvent(parentEventIds.toList(), header, messageGroup.ids(batch), e, e.details + messageGroup.toReadableBody())
                    null
                }
            }
        } catch (throwable: Throwable) {
            val header = "Failed to ${process.actionName} message group"
            return when (process) {
                Process.DECODE -> {
                    val errorEventIds: Map<String, EventID> = eventProcessor.onEachErrorEvent(parentEventIds.toList(), header, messageGroup.ids(batch), throwable)
                    messageGroup.toErrorGroup(batch, header, protocols, throwable, errorEventIds)
                }
                Process.ENCODE -> {
                    // we should not use message IDs because during encoding there is no correct message ID created yet
                    eventProcessor.onEachErrorEvent(parentEventIds.toList(), header, messageGroup.ids(batch), throwable, messageGroup.toReadableBody())
                    null
                }
            }
        } finally {
            when (process) {
                Process.DECODE -> eventProcessor.onEachWarning(parentEventIds, context, "decoding") { messageGroup.ids(batch) }
                Process.ENCODE -> eventProcessor.onEachWarning(parentEventIds, context, "encoding", additionalBody = { messageGroup.toReadableBody() })
            }
        }
    }

    private fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
        incomingProtocols.none { it.isBlank() || containsIgnoreCase(it) } -> false
        incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
        else -> true
    }

    private fun Collection<String>.containsIgnoreCase(item: String) = any { item.equals(it, true) }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}