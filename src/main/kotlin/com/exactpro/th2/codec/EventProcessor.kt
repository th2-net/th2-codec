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
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.utils.message.logId
import mu.KotlinLogging

abstract class AbstractEventProcessor {
    fun onEvent(message: String, messagesIds: List<MessageID> = emptyList(), body: List<String> = emptyList()): String {
        LOGGER.warn { "$message. Messages: ${messagesIds.joinToString(transform = MessageID::logId)}" }
        return storeEvent(message, messagesIds, body)
    }

    fun onErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): String {
        LOGGER.error(cause) { "$message. Messages: ${messagesIds.joinToString(transform = MessageID::logId)}" }
        return storeErrorEvent(message, messagesIds, cause, additionalBody)
    }

    fun onEachEvent(
        events: Set<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = onEvent(message, messagesIds, body)
        storeEachEvent(warnEvent, message, events)
    }

    fun onEachErrorEvent(
        events: Set<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList()
    ): String {
        val errorEventId = onErrorEvent(message, messagesIds, cause, additionalBody)
        storeEachErrorEvent(errorEventId, message, events)
        return errorEventId
    }

    fun onEachWarning(
        warnings: Set<EventID>,
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

    protected abstract fun storeEvent(message: String, messagesIds: List<MessageID>, body: List<String>): String
    protected abstract fun storeErrorEvent(message: String, messagesIds: List<MessageID>, cause: Throwable?, additionalBody: List<String>): String
    protected abstract fun storeEachEvent(warnEvent: String, message: String, events: Set<EventID>)
    protected abstract fun storeEachErrorEvent(errorEventId: String, message: String, events: Set<EventID>)

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}

class StoreEventProcessor(private val storeEventFunc: (Event, EventID?) -> Unit) : AbstractEventProcessor() {

    override fun storeEvent(
        message: String,
        messagesIds: List<MessageID>,
        body: List<String>
    ): String {
        val event = createEvent(message, messagesIds, body = body)
        storeEventFunc(event, null)
        return event.id
    }

    override fun storeErrorEvent(
        message: String,
        messagesIds: List<MessageID>,
        cause: Throwable?,
        additionalBody: List<String>
    ): String {
        val event = createEvent(message, messagesIds, FAILED, cause, additionalBody)
        storeEventFunc(event, null)
        return event.id
    }

    override fun storeEachEvent(
        warnEvent: String,
        message: String,
        events: Set<EventID>
    ) {
        events.forEach {
            it.addReferenceTo(warnEvent, message, PASSED)
        }
    }

    override fun storeEachErrorEvent(
        errorEventId: String,
        message: String,
        events: Set<EventID>
    ) {
        events.forEach {
            it.addReferenceTo(errorEventId, message, FAILED)
        }
    }

    private fun EventID?.addReferenceTo(eventId: String, name: String, status: Status): EventID {
        Event.start()
            .endTimestamp()
            .name(name)
            .status(status)
            .type(if (status != PASSED) "Error" else "Warn")
            .bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
            .bodyData(ReferenceToEvent(eventId))
            .also { event ->
                storeEventFunc(event, this)
            }.run {
                return checkNotNull(EventUtils.toEventID(startTimestamp, "", "", id)) // TODO: bookName & scope
            }
    }

    private fun createEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Status = PASSED,
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ) = Event.start().apply {
        name(message)
        type(if (status != PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) FAILED else status)
        messagesIds.forEach(::messageID)

        if (cause != null) {
            this.exception(cause, true)
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
}