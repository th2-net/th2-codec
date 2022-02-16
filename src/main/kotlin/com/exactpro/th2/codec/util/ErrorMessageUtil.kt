/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.value.toValue

const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"

fun RawMessage.toErrorMessage(protocols: Collection<String>, newParent: EventID?, errorMessage: String) = message().also {
    it.parentEventId = newParent ?: parentEventId

    val protocol = metadata.protocol.ifBlank {
        when (protocols.size) {
            1 -> protocols.first()
            else -> protocols.toString()
        }
    }

    it.metadata = MessageMetadata.newBuilder()
        .setId(metadata.id)
        .setTimestamp(metadata.timestamp)
        .setProtocol(protocol)
        .putAllProperties(metadata.propertiesMap)
        .setMessageType(ERROR_TYPE_MESSAGE)
        .build()

    it.putFields(ERROR_CONTENT_FIELD, errorMessage.toValue())
}

fun MessageGroup.toErrorGroup(infoMessage: String, protocols: Collection<String>, errorEvents: Map<String?, EventID>, throwable: Throwable?): MessageGroup {
    val content = buildString {
        append(infoMessage)
        appendLine("Error for messages: [${messageIds.joinToString(", ") { it.toDebugString() }}] with protocols: [${protocols.joinToString(", ")}]}")
        appendLine("Due to the following errors: ")

        generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    return MessageGroup.newBuilder().also { result ->
        for (anyMessage in this.messagesList) {
            if (anyMessage.kindCase == AnyMessage.KindCase.RAW_MESSAGE && anyMessage.rawMessage.metadata.protocol.run { isBlank() || this in protocols }) {
                result += anyMessage.rawMessage.let { rawMessage ->
                    val eventID = checkNotNull(errorEvents[rawMessage.parentEventIdOrNull]) {
                        "No error event was found for message: ${rawMessage.metadata.id.sequence}"
                    }
                    rawMessage.toErrorMessage(protocols, eventID, content)
                }
            } else {
                result.addMessages(anyMessage)
            }
        }
    }.build()
}