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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.allParsedProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class EncodeProcessor(
    codec: IPipelineCodec,
    private val protocols: Set<String>,
    codecEventID: EventID,
    private val useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    onEvent: (event: ProtoEvent) -> Unit
) : AbstractCodecProcessor(codec, codecEventID, onEvent) {
    private val async = enabledVerticalScaling && Runtime.getRuntime().availableProcessors() > 1
    private val logger = KotlinLogging.logger {}

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<com.exactpro.th2.common.grpc.MessageGroup?>>(source.groupsCount) {
                processMessageGroupAsync(source.getGroups(it))
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(messageBatch::addGroups) }
            }.get()
        } else {
            source.groupsList.forEach { group ->
                processMessageGroup(group)?.run(messageBatch::addGroups)
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount != groupsCount) {
                onErrorEvent("Group count in the decoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    override fun process(source: GroupBatch): GroupBatch {
        val sourceGroups = source.groups
        val resultGroups = mutableListOf<MessageGroup>()

        if (async) {
            val messageGroupFutures = Array(sourceGroups.size) {
                processMessageGroupAsync(source, sourceGroups[it])
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultGroups::add) }
            }.get()
        } else {
            sourceGroups.forEach { group ->
                processMessageGroup(source, group)?.run(resultGroups::add)
            }
        }

        if (sourceGroups.size != resultGroups.size) {
            onErrorEvent("Group count in the encoded batch (${resultGroups.size}) is different from the input one (${sourceGroups.size})")
        }

        return GroupBatch(
            book = source.book,
            sessionGroup = source.sessionGroup,
            groups = resultGroups
        )
    }

    private fun  processMessageGroupAsync(group: ProtoMessageGroup) = CompletableFuture.supplyAsync {
        processMessageGroup(group)
    }

    private fun processMessageGroupAsync(source: GroupBatch, group: MessageGroup) = CompletableFuture.supplyAsync {
        processMessageGroup(source, group)
    }

    private fun processMessageGroup(messageGroup: ProtoMessageGroup): ProtoMessageGroup? {
        if (messageGroup.messagesCount == 0) {
            onErrorEvent("Cannot decode empty message group")
            return null
        }

        if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
            logger.debug { "Message group has no raw messages in it" }
            return messageGroup
        }

        val msgProtocols = messageGroup.allRawProtocols
        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.allParentEventIds else emptySequence()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val decodedMessages = codec.encode(messageGroup, context)

            if (decodedMessages.messagesCount < messageGroup.messagesCount) {
                parentEventIds.onEachEvent("Decoded message group contains less messages (${decodedMessages.messagesCount}) than encoded one (${messageGroup.messagesCount})")
            }

            return decodedMessages
        } catch (e: ValidateException) {
            val header = "Failed to decode: ${e.title}"

            val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, e)
            return messageGroup.toErrorGroup(header, protocols, e, errorEventId)
        } catch (throwable: Throwable) {
            val header = "Failed to decode message group"

            val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, throwable)
            return messageGroup.toErrorGroup(header, protocols, throwable, errorEventId)
        } finally {
            parentEventIds.onEachWarning(context, "decoding") { messageGroup.messageIds }
        }
    }

    private fun processMessageGroup(groupBatch: GroupBatch, messageGroup: MessageGroup): MessageGroup? {
        val messages = messageGroup.messages

        if (messages.isEmpty()) {
            onErrorEvent("Cannot encode empty message group")
            return null
        }

        if (messages.none { it is ParsedMessage }) {
            logger.debug { "Message group has no parsed messages in it" }
            return messageGroup
        }

        val msgProtocols = messageGroup.allParsedProtocols
        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.allParentEventIds else emptySequence()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val encodedGroup = codec.encode(messageGroup, context)

            if (encodedGroup.messages.size > messages.size) {
                parentEventIds.onEachEvent("Encoded message group contains more messages (${encodedGroup.messages.size}) than decoded one (${messages.size})")
            }

            return encodedGroup
        } catch (e: ValidateException) {
            sendErrorEvents("Failed to encode: ${e.title}", parentEventIds, groupBatch, messageGroup, e, e.details)
            return null
        } catch (throwable: Throwable) {
            // we should not use message IDs because during encoding there is no correct message ID created yet
            sendErrorEvents("Failed to encode message group", parentEventIds, groupBatch, messageGroup, throwable, emptyList())
            return null
        } finally {
            parentEventIds.onEachWarning(context, "encoding",
                additionalBody = { messageGroup.toReadableBody() })
        }
    }

    private fun MessageGroup.toReadableBody(): List<String> = messages.map(Message<*>::toJson)

    private fun sendErrorEvents(
        errorMsg: String,
        parentEventIds: Sequence<EventID>,
        groupBatch: GroupBatch,
        msgGroup: MessageGroup,
        cause: Throwable,
        additionalBody: List<String>,
    ) {
        parentEventIds.onEachErrorEvent(errorMsg, msgGroup.messageIds(groupBatch), cause, additionalBody + msgGroup.toReadableBody())
    }
}