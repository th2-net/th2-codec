/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:Suppress("unused")

package com.exactpro.th2.codec.util

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoEventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule

private val mapper = ObjectMapper()

val AnyMessage.parentEventId: EventID?
    get() = when {
        hasMessage() -> with(message) { if (hasParentEventId()) parentEventId else null }
        hasRawMessage() -> with(rawMessage) { if (hasParentEventId()) parentEventId else null }
        else -> error("Unsupported $kindCase kind")
    }

val MessageGroup.parentEventId: EventID?
    get() = messagesList.firstNotNullOfOrNull(AnyMessage::parentEventId)

/**
 * Returns parent event ids from each message.
 */
val DemoMessageGroup.allParentEventIds: Sequence<EventID>
    get() = messages.asSequence()
        .mapNotNull(DemoMessage<*>::eventId)
        .map(DemoEventId::toProto)

val DemoMessageGroup.allRawProtocols: Set<String>
    get() = messages.mapNotNullTo(HashSet()) { (it as? DemoRawMessage)?.protocol }

val DemoMessageGroup.allParsedProtocols: Set<String>
    get() = messages.mapNotNullTo(HashSet()) { (it as? DemoParsedMessage)?.protocol }

val DemoMessageGroup.messageIds: List<MessageID>
    get() = messages.map { it.id.toProto() }

fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
    incomingProtocols.none { it.isBlank() || it in this }  -> false
    incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
    else -> true
}

@Deprecated("Please use the toErrorMessageGroup(exception: Throwable, protocols: List<String>) overload instead", ReplaceWith("this.toErrorMessageGroup(exception, listOf(protocol))"))
fun DemoMessageGroup.toErrorMessageGroup(exception: Throwable, protocol: String): DemoMessageGroup = toErrorMessageGroup(exception, listOf(protocol))

fun DemoMessageGroup.toErrorMessageGroup(exception: Throwable, codecProtocols: Collection<String>): DemoMessageGroup {
    val resultMessages = mutableListOf<DemoMessage<*>>()

    val content = buildString {
        appendLine("$codecProtocols codec has failed to decode one of the following messages: ${messageIds.joinToString(", ") { it.toDebugString() }}")
        appendLine("Due to the following errors: ")

        generateSequence(exception, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    messages.forEach { message ->
        when (message) {
            is DemoParsedMessage -> resultMessages += message
            is DemoRawMessage -> {
                if (message.protocol.run { isBlank() || this in codecProtocols }) {
                    resultMessages += DemoParsedMessage().apply {
                        id = message.id
                        eventId = message.eventId

                        metadata = message.metadata.toMutableMap().apply {
                            this[ERROR_CONTENT_FIELD] = content
                        }

                        protocol = when (codecProtocols.size) {
                            1 -> codecProtocols.first()
                            else -> codecProtocols.toString()
                        }

                        type = ERROR_TYPE_MESSAGE
                    }
                } else {
                    resultMessages += message
                }
            }

            else -> error("${message::class.simpleName} messages are not supported: $message")
        }
    }

    return DemoMessageGroup(resultMessages)
}

fun DemoEventId.toProto(): EventID = EventID.newBuilder().also {
    it.id = id
    it.bookName = book
    it.scope = scope
    it.startTimestamp = timestamp.toTimestamp()
}.build()

fun DemoMessageId.toProto(): MessageID = MessageID.newBuilder().also {
    it.bookName = book
    it.direction = if (direction == INCOMING) FIRST else SECOND
    it.sequence = sequence
    it.timestamp = timestamp.toTimestamp()

    it.addAllSubsequence(subsequence.map(Long::toInt))

    it.connectionIdBuilder.also { connectionId ->
        connectionId.sessionGroup = sessionGroup
        connectionId.sessionAlias = sessionAlias
    }
}.build()

fun DemoMessage<*>.toJson(): String = mapper.registerModule(JavaTimeModule()).writeValueAsString(this)