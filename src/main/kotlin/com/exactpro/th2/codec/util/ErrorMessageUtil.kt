/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.util

import com.exactpro.th2.codec.EventProcessor.Companion.cradleString
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.set
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup

const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"
const val ERROR_EVENT_ID = "error_event_id"

fun RawMessage.toErrorMessage(
    protocols: Collection<String>,
    errorEventId: EventID?,
    errorMessage: String
): ParsedMessage = ParsedMessage(
    id,
    eventId,
    ERROR_TYPE_MESSAGE,
    metadata,
    protocol.ifBlank(protocols::singleOrNull) ?: protocols.toString(),
    hashMapOf(ERROR_CONTENT_FIELD to errorMessage).apply {
        errorEventId?.let { put(ERROR_EVENT_ID, errorEventId.cradleString) }
    }
)

fun MessageGroup.toErrorGroup(
    batch: GroupBatch,
    infoMessage: String,
    protocols: Collection<String>,
    throwable: Throwable,
    bookToEventId: Map<String, EventID>
): MessageGroup {
    val content = buildString {
        appendLine("Error: $infoMessage")
        appendLine("For messages: [${messageIds.joinToString(", ")}] with protocols: $protocols")
        appendLine("Due to the following errors: ")

        generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    val messages = messages.mapTo(ArrayList()) { message ->
        if (message is RawMessage && message.protocol.run { isBlank() || this in protocols }) {
            val book = message.eventId?.book ?: batch.book
            message.toErrorMessage(protocols, bookToEventId[book], content)
        } else {
            message
        }
    }

    return MessageGroup(messages)
}

fun ProtoRawMessage.toErrorMessage(protocols: Collection<String>, errorEventId: EventID?, errorMessage: String) = message().also { builder ->
    val protocol = metadata.protocol.ifBlank(protocols::singleOrNull) ?: protocols.toString()

    builder.metadata = MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
        .setMessageType(ERROR_TYPE_MESSAGE)
        .build()

    builder[ERROR_CONTENT_FIELD] = errorMessage
    errorEventId?.let {
        builder[ERROR_EVENT_ID] = errorEventId
    }
}

fun ProtoMessageGroup.toErrorGroup(
    @Suppress("UNUSED_PARAMETER") batch: MessageGroupBatch,
    infoMessage: String,
    protocols: Collection<String>,
    throwable: Throwable,
    bookToEventId: Map<String, EventID>
): com.exactpro.th2.common.grpc.MessageGroup {
    val content = buildString {
        appendLine("Error: $infoMessage")
        appendLine("For messages: [${messageIds.joinToString { it.toDebugString() }}] with protocols: $protocols")
        appendLine("Due to the following errors: ")

        generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    return com.exactpro.th2.common.grpc.MessageGroup.newBuilder().also { batchBuilder ->
        for (anyMessage in this.messagesList) {
            if (anyMessage.hasRawMessage() && anyMessage.rawMessage.metadata.protocol.run { isBlank() || this in protocols } ) {
                val rawMessage = anyMessage.rawMessage
                val book = if (rawMessage.hasParentEventId()) {
                    rawMessage.parentEventId.bookName
                } else {
                    rawMessage.id.bookName
                }
                batchBuilder += rawMessage.toErrorMessage(protocols, bookToEventId[book], content)
            } else {
                batchBuilder.addMessages(anyMessage)
            }
        }
    }.build()
}