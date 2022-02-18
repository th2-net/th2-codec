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
import com.exactpro.th2.codec.exception.process.DecodeException
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class DecodeProcessor(
    codec: IPipelineCodec,
    private val protocols: Set<String>,
    onEvent: (event: Event, parentId: String?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {

    private val logger = KotlinLogging.logger {}

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesCount == 0) {
                onErrorEvent("Cannot decode empty message group")
                continue
            }

            if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
                logger.debug { "Message group has no raw messages in it" }
                messageBatch.addGroups(messageGroup)
                continue
            }

            val msgProtocols = messageGroup.allRawProtocols
            val parentEventIds = messageGroup.allParentEventIds

            try {
                if (!protocols.checkAgainstProtocols(msgProtocols)) {
                    logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                    messageBatch.addGroups(messageGroup)
                    continue
                }

                val decodedGroup = codec.decode(messageGroup)

                if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                    parentEventIds.forEachEvent("Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(decodedGroup)
            }  catch (processException: DecodeException) {
                //TODO: Exception to events
            } catch (throwable: Throwable) {
                val header = "Failed to decode message group"
                val eventIds = parentEventIds.associateWith { messageEventId ->
                    messageEventId.onErrorEvent(header, messageGroup.messageIds, throwable).id.let { errorEventId ->
                        checkNotNull(EventUtils.toEventID(errorEventId)) {
                            "Failed to create EventID from $errorEventId"
                        }
                    }
                }

                messageBatch.addGroups(messageGroup.toErrorGroup(header, protocols, eventIds, throwable))
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onErrorEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }

}