/*
 *  Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.toErrorMessageGroup
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import java.lang.IllegalStateException

class DecodeProcessor(
    codec: IPipelineCodec,
    private val protocols: List<String>,
    onEvent: (event: Event, parentId: String?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesCount == 0) {
                onErrorEvent("Cannot decode empty message group")
                continue
            }

            val parentEventId = messageGroup.allParentEventIds

            if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
                parentEventId.onErrorEvent("Message group has no parsed messages in it", messageGroup.messageIds)
                continue
            }

            if (!messageGroup.isDecodable()) {
                val info = "No messages of $protocols protocol or mixed empty and non-empty protocols are present"
                parentEventId.onErrorEvent(info, messageGroup.messageIds)
                messageBatch.addGroups(messageGroup.toErrorMessageGroup(IllegalStateException(info), protocols))
                continue
            }

            messageGroup.runCatching(codec::decode).onSuccess { decodedGroup ->
                if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                    parentEventId.onEvent("Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(decodedGroup)
            }.onFailure {
                parentEventId.onErrorEvent("Failed to decode message group", messageGroup.messageIds, it)
                messageBatch.addGroups(messageGroup.toErrorMessageGroup(it, protocols))
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }

    private fun MessageGroup.isDecodable(): Boolean {
        val protocols = messagesList.asSequence()
            .filter(AnyMessage::hasRawMessage)
            .map { it.rawMessage.metadata.protocol }
            .toList()

        return protocols.all(String::isBlank) || protocols.none(String::isBlank) && this@DecodeProcessor.protocols.any { it in protocols }
    }
}