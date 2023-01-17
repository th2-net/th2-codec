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
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.value.toValue

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
val MessageGroup.allParentEventIds: Sequence<EventID>
    get() = messagesList.asSequence()
        .map(AnyMessage::parentEventId)
        .filterNotNull()

val MessageGroup.allRawProtocols
    get() = messagesList.asSequence()
        .filter(AnyMessage::hasRawMessage)
        .map { it.rawMessage.metadata.protocol }
        .toSet()

val MessageGroup.allParsedProtocols
    get() = messagesList.asSequence()
        .filter(AnyMessage::hasMessage)
        .map { it.message.metadata.protocol }
        .toSet()


val MessageGroup.messageIds: List<MessageID>
    get() = messagesList.map { message ->
        when (val kind = message.kindCase) {
            MESSAGE -> message.message.metadata.id
            RAW_MESSAGE -> message.rawMessage.metadata.id
            else -> error("Unknown message kind: $kind")
        }
    }

fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
    incomingProtocols.none { it.isBlank() || it in this }  -> false
    incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
    else -> true
}

@Deprecated("Please use the toErrorMessageGroup(exception: Throwable, protocols: List<String>) overload instead", ReplaceWith("this.toErrorMessageGroup(exception, listOf(protocol))"))
fun MessageGroup.toErrorMessageGroup(exception: Throwable, protocol: String): MessageGroup = this.toErrorMessageGroup(exception, listOf(protocol))

fun MessageGroup.toErrorMessageGroup(exception: Throwable, codecProtocols: Collection<String>): MessageGroup {
    val result = MessageGroup.newBuilder()

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

private fun RawMessage.toMessageMetadataBuilder(protocols: Collection<String>): MessageMetadata.Builder {
    val protocol = metadata.protocol.ifBlank {
        when(protocols.size) {
            1 -> protocols.first()
            else -> protocols.toString()
        }
    }

    return MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}

private fun Message.toRawMetadataBuilder(protocols: Collection<String>): RawMessageMetadata.Builder {
    val protocol = metadata.protocol.ifBlank {
        when(protocols.size) {
            1 -> protocols.first()
            else -> protocols.toString()
        }
    }

    return RawMessageMetadata.newBuilder()
        .setId(metadata.id)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}