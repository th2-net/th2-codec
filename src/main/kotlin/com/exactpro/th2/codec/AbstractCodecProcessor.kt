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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.utils.event.logId
import mu.KotlinLogging

abstract class AbstractCodecProcessor(
    protected val codec: IPipelineCodec,
    private val codecEventID: EventID,
    private val onEvent: (event: ProtoEvent) -> Unit,
) : MessageProcessor<GroupBatch, GroupBatch> {
    private val logger = KotlinLogging.logger {}

    protected fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()) = codecEventID.onEvent(message, messagesIds)

    protected fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = codecEventID.onErrorEvent(message, messagesIds, cause)

    private fun EventID.onEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList(),
    ) : EventID {
        logger.warn { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        return this.publishEvent(message, messagesIds, body = body).id
    }

    private fun EventID.onErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): EventID {
        logger.error(cause) { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        return publishEvent(message, messagesIds, FAILED, cause, additionalBody).id
    }

    protected fun Sequence<EventID>.onEachEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = codecEventID.onEvent(message, messagesIds, body)
        forEach {
            it.addReferenceTo(warnEvent, message, PASSED)
        }
    }

    protected fun Sequence<EventID>.onEachErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList(),
    ): EventID {
        val errorEventId = codecEventID.onErrorEvent(message, messagesIds, cause, additionalBody)
        forEach { it.addReferenceTo(errorEventId, message, FAILED) }
        return errorEventId
    }

    protected fun Sequence<EventID>.onEachWarning(
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

    protected fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
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
}