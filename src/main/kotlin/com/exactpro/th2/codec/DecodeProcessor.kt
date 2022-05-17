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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class DecodeProcessor(
    codec: IPipelineCodec,
    eventProcessor: AbstractEventProcessor,
    private val protocols: Set<String>,
    private val useParentEventId: Boolean = true
) : AbstractCodecProcessor(codec, eventProcessor) {

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesCount == 0) {
                eventProcessor.onErrorEvent("Cannot decode empty message group")
                continue
            }

            if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
                LOGGER.debug { "Message group has no raw messages in it" }
                messageBatch.addGroups(messageGroup)
                continue
            }

            val msgProtocols = messageGroup.allRawProtocols
            val parentEventIds = if (useParentEventId) messageGroup.allParentEventIds else emptySet()
            val context = ReportingContext()

            try {
                if (!protocols.checkAgainstProtocols(msgProtocols)) {
                    LOGGER.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                    messageBatch.addGroups(messageGroup)
                    continue
                }

                val decodedGroup = codec.decode(messageGroup, context)

                if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                    eventProcessor.onEachEvent(parentEventIds, "Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(decodedGroup)
            } catch (e: ValidateException) {
                val header = "Failed to decode: ${e.title}"

                eventProcessor.onEachErrorEvent(parentEventIds, header, messageGroup.messageIds, e)
                val eventIds = parentEventIds // TODO: fix - get real eventIds?

                messageBatch.addGroups(header, messageGroup, parentEventIds, e, eventIds)
            } catch (throwable: Throwable) {
                val header = "Failed to decode message group"

                eventProcessor.onEachErrorEvent(parentEventIds, header, messageGroup.messageIds, throwable)
                val eventIds = parentEventIds // TODO: fix - get real eventIds?

                messageBatch.addGroups(header, messageGroup, parentEventIds, throwable, eventIds)
            }

            eventProcessor.onEachWarning(parentEventIds, context, "decoding") { messageGroup.messageIds }
        }

        return messageBatch.build().apply {
            if (source.groupsCount != groupsCount) {
                eventProcessor.onErrorEvent("Group count in the decoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    //  TODO: fix - zipping of unordered collections
    private fun MessageGroupBatch.Builder.addGroups(header: String,
                                                    messageGroup: MessageGroup,
                                                    parentEventIds: Set<String>,
                                                    throwable: Throwable,
                                                    eventIds: Set<String>) {
        val eventIdsMap: Map<String, EventID> = (parentEventIds zip eventIds).associate {
            it.first to EventID.newBuilder().setId(it.second).build()
        }

        addGroups(messageGroup.toErrorGroup(header, protocols, eventIdsMap, throwable, useParentEventId))
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}