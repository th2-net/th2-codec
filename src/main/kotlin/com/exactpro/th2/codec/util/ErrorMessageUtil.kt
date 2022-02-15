package com.exactpro.th2.codec.util

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.TextFormat

const val ERROR_TYPE_MESSAGE = "th2-codec-error"
const val ERROR_CONTENT_FIELD = "content"

fun RawMessage.toErrorMessage(protocols: Collection<String>, newParent: EventID?, errorMessage: String) = message().also {
    it.parentEventId = newParent ?: parentEventId

    val protocol = metadata.protocol.ifBlank {
        when(protocols.size) {
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

fun MessageGroup.toErrorGroup(infoMessage: String, codecProtocols: Collection<String>, errorEvents: Map<String, EventID>, throwable: Throwable?, relationMethod: (AnyMessage) -> Boolean): MessageGroup {
    val content = buildString {
        append(infoMessage)
        appendLine("Error for messages: [${messageIds.joinToString(", ") { it.toDebugString() }}] with protocols: [${codecProtocols.joinToString(", ")}]}")
        appendLine("Due to the following errors: ")

        generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
            appendLine("$index: ${cause.message}")
        }
    }

    return MessageGroup.newBuilder().also { result ->
        this.messagesList.forEach {
            if (relationMethod(it)) {
                result += when(it.kindCase) {
                    AnyMessage.KindCase.RAW_MESSAGE -> it.rawMessage.let { rawMessage ->
                        val eventID = if (rawMessage.hasParentEventId()) {
                            checkNotNull(errorEvents[rawMessage.parentEventId.id]) {"No error event was found for message: ${rawMessage.metadata.id.sequence}"}
                        } else {
                            null
                        }
                        rawMessage.toErrorMessage(codecProtocols, eventID, content)
                    }
                    // Parsed messages to errors not supported right now
                    else -> error("Unprotected kind of message ${it.kindCase} in message: ${TextFormat.shortDebugString(it.message)}")
                }
            } else {
                result.addMessages(it)
            }
        }
    }.build()
}