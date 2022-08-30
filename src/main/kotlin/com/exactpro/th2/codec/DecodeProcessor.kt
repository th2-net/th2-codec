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
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class DecodeProcessor(
    codec: IPipelineCodec,
    eventProcessor: AbstractEventProcessor,
    private val protocols: Set<String>,
    private val useParentEventId: Boolean = true,
) : AbstractCodecProcessor(codec, eventProcessor) {
    private val async = Runtime.getRuntime().availableProcessors() > 1

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

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
                eventProcessor.onErrorEvent("Group count in the decoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    private fun processMessageGroupAsync(group: MessageGroup) = CompletableFuture.supplyAsync { processMessageGroup(group) }

    private fun processMessageGroup(messageGroup: MessageGroup): MessageGroup? {
        if (messageGroup.messagesCount == 0) {
            eventProcessor.onErrorEvent("Cannot decode empty message group")
            return null
        }

        if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
            LOGGER.debug { "Message group has no raw messages in it" }
            return messageGroup
        }

        val msgProtocols: Set<String> = messageGroup.allRawProtocols
        val parentEventIds = if (useParentEventId) messageGroup.allParentEventIds else emptySet()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                LOGGER.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val decodedGroup = codec.decode(messageGroup, context)

            if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                eventProcessor.onEachEvent(parentEventIds, "Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
            }

            return decodedGroup
        } catch (e: ValidateException) {
            val header = "Failed to decode: ${e.title}"
            val errorEventId = eventProcessor.onEachErrorEvent(parentEventIds, header, messageGroup.messageIds, e)
            return messageGroup.toErrorGroup(header, protocols, e, errorEventId)
        } catch (throwable: Throwable) {
            val header = "Failed to decode message group"
            val errorEventId = eventProcessor.onEachErrorEvent(parentEventIds, header, messageGroup.messageIds, throwable)
            return messageGroup.toErrorGroup(header, protocols, throwable, errorEventId)
        } finally {
            eventProcessor.onEachWarning(parentEventIds, context, "decoding") { messageGroup.messageIds }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
