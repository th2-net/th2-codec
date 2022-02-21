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
import com.exactpro.th2.codec.util.checkAgainstProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class EncodeProcessor(
    codec: IPipelineCodec,
    private val protocols: Set<String>,
    private val isGeneral: Boolean = false,
    onEvent: (event: Event, parentId: String?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {

    private val logger = KotlinLogging.logger {}

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesCount == 0) {
                onErrorEvent("Cannot encode empty message group")
                continue
            }

            if (messageGroup.messagesList.none(AnyMessage::hasMessage)) {
                logger.debug { "Message group has no parsed messages in it" }
                messageBatch.addGroups(messageGroup)
                continue
            }

            val msgProtocols = messageGroup.allParsedProtocols
            val parentEventId = if (isGeneral) emptySet() else messageGroup.allParentEventIds
            val context = ReportingContext()

            try {
                if (!protocols.checkAgainstProtocols(msgProtocols)) {
                    logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                    messageBatch.addGroups(messageGroup)
                    continue
                }

                val encodedGroup = codec.encode(messageGroup, context)

                if (encodedGroup.messagesCount > messageGroup.messagesCount) {
                    parentEventId.onEachEvent("Encoded message group contains more messages (${encodedGroup.messagesCount}) than decoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(encodedGroup)
            } catch (throwable: Throwable) {
                // we should not use message IDs because during encoding there is no correct message ID created yet
                parentEventId.onEachErrorEvent("Failed to encode message group", cause = throwable)
            }

            parentEventId.onEachWarning(context, "encoding") { messageGroup.messageIds }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }


}