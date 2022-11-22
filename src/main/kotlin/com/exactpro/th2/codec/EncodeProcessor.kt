/*
 *  Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.toJson
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class EncodeProcessor(
    codec: IPipelineCodec,
    eventProcessor: AbstractEventProcessor,
    private val protocols: Set<String>,
    private val useParentEventId: Boolean = true,
) : AbstractCodecProcessor(codec, eventProcessor) {
    private val async = Runtime.getRuntime().availableProcessors() > 1

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()
            .setMetadata(source.metadata)

        if (async) {
            val messageGroupFutures = Array<CompletableFuture<MessageGroup?>>(source.groupsCount) {
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
                eventProcessor.onErrorEvent("Group count in the encoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    private fun processMessageGroupAsync(group: MessageGroup) = CompletableFuture.supplyAsync { processMessageGroup(group) }

    private fun processMessageGroup(messageGroup: MessageGroup): MessageGroup? {
        if (messageGroup.messagesCount == 0) {
            eventProcessor.onErrorEvent("Cannot encode empty message group")
            return null
        }

        if (messageGroup.messagesList.none(AnyMessage::hasMessage)) {
            LOGGER.debug { "Message group has no parsed messages in it" }
            return messageGroup
        }

        val msgProtocols: Set<String> = messageGroup.allParsedProtocols
        val parentEventIds = if (useParentEventId) messageGroup.allParentEventIds else emptySet()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                LOGGER.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val encodedGroup = codec.encode(messageGroup, context)

            if (encodedGroup.messagesCount > messageGroup.messagesCount) {
                eventProcessor.onEachEvent(
                    parentEventIds, "Encoded message group contains more messages (${encodedGroup.messagesCount}) than decoded one (${messageGroup.messagesCount})"
                )
            }

            return encodedGroup
        } catch (e: ValidateException) {
            sendErrorEvents("Failed to encode: ${e.title}", parentEventIds, messageGroup, e, e.details)
            return null
        } catch (throwable: Throwable) {
            // we should not use message IDs because during encoding there is no correct message ID created yet
            sendErrorEvents("Failed to encode message group", parentEventIds, messageGroup, throwable, emptyList())
            return null
        } finally {
            eventProcessor.onEachWarning(parentEventIds, context, "encoding", additionalBody = { messageGroup.toReadableBody(false) })
        }
    }

    private fun MessageGroup.toReadableBody(shortFormat: Boolean): List<String> = mutableListOf<String>().apply {
        messagesList.forEach {
            when {
                it.hasRawMessage() -> add(it.rawMessage.toJson(shortFormat))
                it.hasMessage() -> add(it.message.toJson(shortFormat))
            }
        }
    }

    private fun sendErrorEvents(
        errorMsg: String, parentEventIds: Set<String>, msgGroup: MessageGroup,
        cause: Throwable, additionalBody: List<String>,
    ) {
        eventProcessor.onEachErrorEvent(
            parentEventIds,
            errorMsg,
            msgGroup.messageIds,
            cause,
            additionalBody + msgGroup.toReadableBody(false)
        )
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
