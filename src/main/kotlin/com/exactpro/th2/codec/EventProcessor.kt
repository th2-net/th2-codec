/*
 *  Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.MessageID
import mu.KotlinLogging

class EventProcessor(onEvent: ((event: Event, parentId: String?) -> Unit)?) {
    private val onEvent = onEvent ?:  { _, _ -> }

    fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()) =
        null.onEvent(message, messagesIds)

    fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = null.onErrorEvent(message, messagesIds, cause)

    private fun String?.onEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ): String {
        LOGGER.warn { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        val event = createEvent(message, messagesIds, body = body)
        onEvent(event, this)
        return event.id
    }

    private fun String?.onErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): String {
        LOGGER.error(cause) { "$message. Messages: ${messagesIds.joinToReadableString()}" }
        val event = createEvent(message, messagesIds, Event.Status.FAILED, cause, additionalBody)
        onEvent(event, this)
        return event.id
    }

    fun onEachEvent(
        events: Set<String>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = null.onEvent(message, messagesIds, body)
        events.forEach {
            it.addReferenceTo(warnEvent, message, Event.Status.PASSED)
        }
    }

    fun onEachErrorEvent(
        events: Set<String>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList(),
    ) {
        val errorEventId = null.onErrorEvent(message, messagesIds, cause, additionalBody)
        events.forEach {
            it.addReferenceTo(errorEventId, message, Event.Status.FAILED)
        }
    }

    fun onEachWarning(
        warnings: Set<String>,
        context: ReportingContext,
        action: String,
        additionalBody: () -> List<String> = ::emptyList,
        messagesIds: () -> List<MessageID> = ::emptyList
    ) = context.warnings.let {
        if (it.isNotEmpty()) {
            val messages = messagesIds()
            val body = additionalBody()
            it.forEach { warning ->
                onEachEvent(warnings, "[WARNING] During $action: $warning", messages, body)
            }
        }
    }

    private fun List<MessageID>.joinToReadableString(): String =
        joinToString(", ") {
            "${it.connectionId.sessionAlias}:${it.direction}:${it.sequence}[.${it.subsequenceList.joinToString(".")}]"
        }

    private fun String.addReferenceTo(eventId: String, name: String, status: Event.Status) {
        onEvent(
            Event.start()
                .endTimestamp()
                .name(name)
                .status(status)
                .type(if (status != Event.Status.PASSED) "Error" else "Warn")
                .bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
                .bodyData(ReferenceToEvent(eventId)),
            this
        )
    }

    private fun createEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Event.Status = Event.Status.PASSED,
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ) = Event.start().apply {
        name(message)
        type(if (status != Event.Status.PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) Event.Status.FAILED else status)
        messagesIds.forEach(::messageID)

        generateSequence(cause, Throwable::cause).forEach {
            bodyData(EventUtils.createMessageBean(it.message))
        }

        if (body.isNotEmpty()) {
            bodyData(EventUtils.createMessageBean("Information:"))
            body.forEach { bodyData(EventUtils.createMessageBean(it)) }
        }
    }

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