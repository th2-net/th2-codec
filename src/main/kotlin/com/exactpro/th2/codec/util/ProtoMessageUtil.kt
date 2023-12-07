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
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.AnyMessage as ProtoAnyMessage
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.grpc.MessageMetadata as ProtoMessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata as ProtoRawMessageMetadata
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID
import com.exactpro.th2.common.grpc.EventID as ProtoEventID
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.value.toValue

val ProtoAnyMessage.parentEventId: ProtoEventID?
    get() = when {
        hasMessage() -> with(message) { if (hasParentEventId()) parentEventId else null }
        hasRawMessage() -> with(rawMessage) { if (hasParentEventId()) parentEventId else null }
        else -> error("Unsupported $kindCase kind")
    }

val ProtoMessageGroup.parentEventId: ProtoEventID?
    get() = messagesList.firstNotNullOfOrNull(ProtoAnyMessage::parentEventId)

/**
 * Returns parent event ids from each message.
 */
val ProtoMessageGroup.allParentEventIds: Sequence<ProtoEventID>
    get() = messagesList.asSequence()
        .mapNotNull(ProtoAnyMessage::parentEventId)

val ProtoMessageGroup.allRawProtocols
    get() = messagesList.asSequence()
        .filter(ProtoAnyMessage::hasRawMessage)
        .map { it.rawMessage.metadata.protocol }
        .toSet()

val ProtoMessageGroup.allParsedProtocols
    get() = messagesList.asSequence()
        .filter(ProtoAnyMessage::hasMessage)
        .map { it.message.metadata.protocol }
        .toSet()

val ProtoMessageGroup.messageIds: List<ProtoMessageID>
    get() = messagesList.map(AnyMessage::id)

val AnyMessage.id: MessageID
    get() = when (val kind = kindCase) {
        MESSAGE -> message.metadata.id
        RAW_MESSAGE -> rawMessage.metadata.id
        else -> error("Unknown message kind: $kind")
    }

@Deprecated("Please use the toErrorMessageGroup(exception: Throwable, protocols: List<String>) overload instead", ReplaceWith("this.toErrorMessageGroup(exception, listOf(protocol))"))
fun ProtoMessageGroup.toErrorMessageGroup(exception: Throwable, protocol: String): ProtoMessageGroup = this.toErrorMessageGroup(exception, listOf(protocol))

fun ProtoMessageGroup.toErrorMessageGroup(exception: Throwable, codecProtocols: Collection<String>): ProtoMessageGroup {
    val result = ProtoMessageGroup.newBuilder()

    val content = buildString {
        appendLine("$codecProtocols codec has failed to decode one of the following messages: ${messageIds.joinToString(", ") { it.toDebugString() }}")
        appendLine("Due to the following errors: ")

        generateSequence(exception, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    this.messagesList.forEach { message ->
        when {
            message.hasMessage() -> result.addMessages(message)
            message.hasRawMessage() -> {
                message.rawMessage.let { rawMessage ->
                    if (rawMessage.metadata.protocol.run { isBlank() || this in codecProtocols }) {
                        result += message().apply {
                            if (rawMessage.hasParentEventId()) {
                                parentEventId = rawMessage.parentEventId
                            }
                            metadata = rawMessage.toMessageMetadataBuilder(codecProtocols)
                                .setMessageType(ERROR_TYPE_MESSAGE)
                                .build()
                            putFields(ERROR_CONTENT_FIELD, content.toValue())
                        }
                    } else {
                        result.addMessages(message)
                    }
                }
            }
            else -> error("${message.kindCase} messages are not supported: ${message.toJson(true)}")
        }
    }
    return result.build()
}

fun ProtoRawMessage.toMessageMetadataBuilder(protocols: Collection<String>): ProtoMessageMetadata.Builder {
    val protocol = metadata.protocol.ifBlank {
        when(protocols.size) {
            1 -> protocols.first()
            else -> protocols.toString()
        }
    }

    return ProtoMessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}

fun ProtoMessage.toRawMetadataBuilder(protocols: Collection<String>): ProtoRawMessageMetadata.Builder {
    val protocol = metadata.protocol.ifBlank {
        when(protocols.size) {
            1 -> protocols.first()
            else -> protocols.toString()
        }
    }

    return ProtoRawMessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}