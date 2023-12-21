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

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.exactpro.th2.common.grpc.EventID as ProtoEventID
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID

private val mapper = ObjectMapper()

/**
 * Returns parent event ids from each message.
 */
val MessageGroup.allParentEventIds: Sequence<ProtoEventID>
    get() = messages.asSequence()
        .mapNotNull(Message<*>::eventId)
        .map(EventId::toProto)

val MessageGroup.allRawProtocols: Set<String>
    get() = messages.mapNotNullTo(HashSet()) { (it as? RawMessage)?.protocol }

val MessageGroup.allParsedProtocols: Set<String>
    get() = messages.mapNotNullTo(HashSet()) { (it as? ParsedMessage)?.protocol }

val MessageGroup.messageIds: List<MessageId>
    get() = messages.map(Message<*>::id)
fun MessageGroup.messageIds(groupBatch: GroupBatch): List<ProtoMessageID> = messages.map { it.id.toProto(groupBatch) }

fun MessageGroup.extractPairIds(groupBatch: GroupBatch): Map<ProtoMessageID, ProtoEventID?> = messages.associate {
    it.id.toProto(groupBatch) to it.eventId?.toProto()
}

fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
    incomingProtocols.none { it.isBlank() || it in this }  -> false
    incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
    else -> true
}

@Deprecated("Please use the toErrorMessageGroup(exception: Throwable, protocols: List<String>) overload instead", ReplaceWith("this.toErrorMessageGroup(exception, listOf(protocol))"))
fun MessageGroup.toErrorMessageGroup(exception: Throwable, protocol: String): MessageGroup = toErrorMessageGroup(
    exception,
    listOf(protocol)
)

fun MessageGroup.toErrorMessageGroup(exception: Throwable, codecProtocols: Collection<String>): MessageGroup {
    val resultMessages = mutableListOf<Message<*>>()

    val content = buildString {
        appendLine("$codecProtocols codec has failed to decode one of the following messages: ${messageIds.joinToString(", ")}")
        appendLine("Due to the following errors: ")

        generateSequence(exception, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    messages.forEach { message ->
        when (message) {
            is ParsedMessage -> resultMessages += message
            is RawMessage -> {
                resultMessages += if (message.protocol.run { isBlank() || this in codecProtocols }) {
                    ParsedMessage(
                        message.id,
                        message.eventId,
                        ERROR_TYPE_MESSAGE,
                        message.metadata.toMutableMap().apply { this[ERROR_CONTENT_FIELD] = content },
                        when (codecProtocols.size) {
                            1 -> codecProtocols.first()
                            else -> codecProtocols.toString()
                        },
                    )
                } else {
                    message
                }
            }

            else -> error("${message::class.simpleName} messages are not supported: $message")
        }
    }

    return MessageGroup(resultMessages)
}

fun EventId.toProto(): ProtoEventID = ProtoEventID.newBuilder().also {
    it.id = id
    it.bookName = book
    it.scope = scope
    it.startTimestamp = timestamp.toTimestamp()
}.build()

fun MessageId.toProto(groupBatch: GroupBatch): ProtoMessageID = ProtoMessageID.newBuilder().also {
    it.bookName = groupBatch.book
    it.direction = if (direction == Direction.INCOMING) FIRST else SECOND
    it.sequence = sequence
    it.timestamp = timestamp.toTimestamp()

    it.addAllSubsequence(subsequence)

    it.connectionIdBuilder.also { connectionId ->
        connectionId.sessionGroup = groupBatch.sessionGroup
        connectionId.sessionAlias = sessionAlias
    }
}.build()

fun Message<*>.toJson(): String = mapper.registerModule(JavaTimeModule()).writeValueAsString(this)