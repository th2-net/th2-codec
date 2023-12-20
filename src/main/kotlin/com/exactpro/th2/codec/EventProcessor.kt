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

import com.exactpro.cradle.utils.TimeUtils
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.isValid
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.toInstant
import com.google.protobuf.TimestampOrBuilder
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class EventProcessor(
    componentBook: String,
    private val componentName: String,
    private val onEvent: (event: ProtoEvent) -> Unit
) {
    private val creationTime: String = Instant.now().toString()
    private val rootEvents = ConcurrentHashMap<String, EventID>()
    val codecEventID = getRootEvent(componentBook)

    @JvmOverloads
    fun onEvent(event: Event, parentEventId: EventID = codecEventID) {
        onEvent(event.toProto(parentEventId))
    }

    fun onEachEvent(
        parentEventIDs: List<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        chooseRootEventsForPublication(parentEventIDs, messagesIds).forEach { (codecEventId, externalEventIds) ->
            val warnEvent = codecEventId.onEvent(message, messagesIds, body)
            externalEventIds.forEach {
                it.addReferenceTo(warnEvent, message, Event.Status.PASSED, messagesIds, additionalBody = body)
            }
        }
    }

    fun onEachWarning(
        parentEventIDs: Sequence<EventID>,
        context: ReportingContext,
        action: String,
        additionalBody: () -> List<String> = ::emptyList,
        messagesIds: () -> List<MessageID> = ::emptyList
    ) {
        context.warnings.let { warnings ->
            if (warnings.isNotEmpty()) {
                val messages = messagesIds()
                val body = additionalBody()
                warnings.forEach { warning ->
                    onEachEvent(parentEventIDs.toList(), "[WARNING] During $action: $warning", messages, body)
                }
            }
        }
    }

    fun onErrorEvent(
        eventId: EventID? = null,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null
    ): EventID = (eventId ?: codecEventID).onErrorEvent(message, messagesIds, cause)

    fun onEachErrorEvent(
        parentEventIDs: List<EventID>,
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ): Map<String, EventID> {
        val bookToEventId = mutableMapOf<String, EventID>()
        chooseRootEventsForPublication(parentEventIDs, messagesIds).forEach { (codecEventId, externalEventIds) ->
            val errorEventId = codecEventId.onErrorEvent(message, messagesIds, cause, body)
            bookToEventId.put(errorEventId.bookName, errorEventId)?.also {
                error(
                    "Internal error: several root events have been chosen for parent event ids: $parentEventIDs and message ids: $messagesIds"
                )
            }
            externalEventIds.forEach {
                it.addReferenceTo(errorEventId, message, Event.Status.FAILED, messagesIds, cause, body)
            }
        }
        return bookToEventId
    }

    internal fun chooseRootEventsForPublication(
        eventIds: List<EventID>,
        messagesIds: List<MessageID>,
    ): Map<EventID, List<EventID>> = mutableMapOf<EventID, MutableList<EventID>>().apply {
        eventIds.asSequence()
            .distinct()
            .forEach { eventId ->
                compute(getRootEvent(eventId.bookName)) { _, value ->
                    (value ?: mutableListOf()).apply {
                        add(eventId)
                    }
                }
            }
        messagesIds.asSequence()
            .map(MessageID::getBookName)
            .distinct()
            .forEach { book ->
                putIfAbsent(getRootEvent(book), mutableListOf())
            }

        if (isEmpty()) {
            put(codecEventID, mutableListOf())
        }
    }

    private fun getRootEvent(book: String): EventID = rootEvents.computeIfAbsent(book) {
        Event.start()
            .endTimestamp()
            .name("$componentName $creationTime")
            .description("Root event")
            .status(Event.Status.PASSED)
            .type("Microservice")
            .toProto(book, componentName)
            .also(onEvent)
            .id
    }

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

    private fun EventID.addReferenceTo(
        eventId: EventID,
        name: String,
        status: Event.Status,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null,
        additionalBody: List<String> = emptyList(),
    ): EventID = Event.start().apply {
        endTimestamp()
        name(name)
        status(status)
        type(if (status != Event.Status.PASSED) "Error" else "Warn")
        bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
        bodyData(ReferenceToEvent(eventId.cradleString))
        fill(bookName, messagesIds, cause, additionalBody)
    }.toProto(this)
        .also(onEvent)
        .id

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
        fill(bookName, messagesIds, cause, body)
    }.toProto(this).also(onEvent)

    private fun Event.fill(
        book: String,
        messagesIds: List<MessageID>,
        cause: Throwable?,
        body: List<String>
    ) {
        var addReferenceToMessages = false
        messagesIds.forEach { messageId ->
            if (messageId.isValid) {
                if (book == messageId.bookName) {
                    messageID(messageId)
                } else {
                    if (!addReferenceToMessages) {
                        addReferenceToMessages = true
                        bodyData(EventUtils.createMessageBean("This event contains reference to messages from another book"))
                    }
                    bodyData(ReferenceToMessage(messageId.cradleString))
                }
            }
        }

        if (cause != null) {
            exception(cause, true)
        }

        if (body.isNotEmpty()) {
            bodyData(EventUtils.createMessageBean("Information:"))
            body.forEach { bodyData(EventUtils.createMessageBean(it)) }
        }
    }

    @Suppress("unused")
    private class ReferenceToEvent(val eventId: String) : IBodyData {
        val type: String
            get() = TYPE

        companion object {
            const val TYPE = "reference"
        }
    }

    @Suppress("unused")
    private class ReferenceToMessage(val messageId: String) : IBodyData {
        val type: String
            get() = TYPE

        companion object {
            const val TYPE = "reference"
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        private val TimestampOrBuilder.cradleString get() = TimeUtils.toIdTimestamp(toInstant())
        private val Direction.cradleString get() = if (Direction.FIRST == this) "1" else "2"
        internal val EventID.cradleString get() = "$bookName:$scope:${startTimestamp.cradleString}:$id"
        internal val MessageID.cradleString get() = "$bookName:$sessionAlias:${direction.cradleString}:${timestamp.cradleString}:$sequence"
    }
}