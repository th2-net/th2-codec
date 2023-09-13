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

import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.isValid
import com.exactpro.th2.common.utils.message.logId
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging

class EventProcessor(
    private val codecEventID: EventID,
    private val onEvent: (event: ProtoEvent) -> Unit
) {
    fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()) = codecEventID.onEvent(message, messagesIds)
    fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = codecEventID.onErrorEvent(message, messagesIds, cause)

    private fun EventID.onEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList(),
    ) : EventID {
        LOGGER.warn { "$message. Messages: ${messagesIds.joinToString(transform = MessageID::logId)}" }
        return this.publishEvent(message, messagesIds, body = body).id
    }

    private fun EventID.onErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): EventID {
        LOGGER.error(cause) { "$message. Messages: ${messagesIds.joinToString(transform = MessageID::logId)}" }
        return publishEvent(message, messagesIds, Event.Status.FAILED, cause, additionalBody).id
    }

    fun onEachEvent(
        parentEventIDs: Sequence<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = codecEventID.onEvent(message, messagesIds, body)
        parentEventIDs.forEach {
            it.addReferenceTo(warnEvent, message, Event.Status.PASSED)
        }
    }

    fun onEachErrorEvent(
        parentEventIDs: Sequence<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList(),
    ): EventID {
        val errorEventId = codecEventID.onErrorEvent(message, messagesIds, cause, additionalBody)
        parentEventIDs.forEach { it.addReferenceTo(errorEventId, message, Event.Status.FAILED) }
        return errorEventId
    }

    fun onEachWarning(
        parentEventIDs: Sequence<EventID>,
        context: ReportingContext,
        action: String,
        additionalBody: () -> List<String> = ::emptyList,
        messagesIds: () -> List<MessageID> = ::emptyList
    ) = context.warnings.let { warnings ->
        if (warnings.isNotEmpty()) {
            val messages = messagesIds()
            val body = additionalBody()
            warnings.forEach { warning ->
                onEachEvent(parentEventIDs, "[WARNING] During $action: $warning", messages, body)
            }
        }
    }

    private fun EventID.addReferenceTo(eventId: EventID, name: String, status: Event.Status): EventID = Event.start()
        .endTimestamp()
        .name(name)
        .status(status)
        .type(if (status != Event.Status.PASSED) "Error" else "Warn")
        .bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
        .bodyData(ReferenceToEvent(eventId.idString))
        .toProto(this)
        .also(onEvent)
        .id

    private val EventID.idString get() = "${bookName}:${scope}:${Timestamps.toString(startTimestamp)}:${id}"

    private fun EventID.publishEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Event.Status = Event.Status.PASSED,
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ): ProtoEvent = Event.start().apply {
        name(message)
        type(if (status != Event.Status.PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) Event.Status.FAILED else status)
        messagesIds.forEach { messageId ->
            if (messageId.isValid) {
                messageID(messageId)
            }
        }

        if (cause != null) {
            exception(cause, true)
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