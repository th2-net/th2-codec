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
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorMessageGroup
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class EncodeProcessor(
    codec: IPipelineCodec,
    private val protocols: List<String>,
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

            val parentEventId = messageGroup.allParentEventIds

            if (messageGroup.messagesList.none(AnyMessage::hasMessage)) {
                logger.debug { "Message group has no parsed messages in it" }
                messageBatch.addGroups(messageGroup)
                continue
            }

            val msgProtocols = messageGroup.protocols
            if (msgProtocols.none(String::isBlank) && protocols.none { it in msgProtocols }) {
                logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                messageBatch.addGroups(messageGroup)
                continue
            } else if (msgProtocols.all(String::isBlank) || msgProtocols.none(String::isBlank) && protocols.any { it in msgProtocols }) {
                // do nothing
            } else {
                val info = "No messages of $protocols protocols or mixed empty and non-empty protocols are present"
                parentEventId.onErrorEvent(info, messageGroup.messageIds)
                continue
            }

            messageGroup.runCatching(codec::encode).onSuccess { encodedGroup ->
                if (encodedGroup.messagesCount > messageGroup.messagesCount) {
                    parentEventId.onEvent("Encoded message group contains more messages (${encodedGroup.messagesCount}) than decoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(encodedGroup)
            }.onFailure {
                parentEventId.onErrorEvent("Failed to encode message group", messageGroup.messageIds, it)
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }

    private val MessageGroup.protocols
        get() = messagesList.asSequence()
            .filter(AnyMessage::hasMessage)
            .map { it.message.metadata.protocol }
            .toList()

}