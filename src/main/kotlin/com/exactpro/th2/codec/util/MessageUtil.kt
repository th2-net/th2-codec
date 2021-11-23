/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.value.toValue


const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"

val MessageGroup.parentEventId: String?
    get() = messagesList.asSequence()
        .map {
            when {
                it.hasMessage() -> it.message.parentEventId.id.ifEmpty { null }
                it.hasRawMessage() -> it.rawMessage.parentEventId.id.ifEmpty { null }
                else -> null
            }
        }
        .firstOrNull { it != null }

val MessageGroup.messageIds: List<MessageID>
    get() = messagesList.map { message ->
        when (val kind = message.kindCase) {
            MESSAGE -> message.message.metadata.id
            RAW_MESSAGE -> message.rawMessage.metadata.id
            else -> error("Unknown message kind: $kind")
        }
    }

fun MessageGroup.toErrorMessageGroup(exception: Throwable, protocol: String) : MessageGroup {
    val result = MessageGroup.newBuilder()

    val content = buildString {
        appendLine("$protocol codec has failed to decode one of the following messages: ${messageIds.joinToString(", ") { it.toDebugString() }}")
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
                    if (rawMessage.metadata.protocol.run { isBlank() || this == protocol }) {
                        result += message().apply {
                            if (rawMessage.hasParentEventId()) {
                                parentEventId = rawMessage.parentEventId
                            }
                            metadata = rawMessage.toMessageMetadataBuilder(protocol).setMessageType(ERROR_TYPE_MESSAGE).build()
                            putFields(ERROR_CONTENT_FIELD, content.toValue())
                        }
                    } else {
                        result.addMessages(message)
                    }
                }
            }
        }
    }
    return result.build()
}

fun RawMessage.toMessageMetadataBuilder(protocol: String): MessageMetadata.Builder {
    return MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setTimestamp(metadata.timestamp)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
}