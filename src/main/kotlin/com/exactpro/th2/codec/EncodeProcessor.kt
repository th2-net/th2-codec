/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.parentEventId
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class EncodeProcessor(
    codec: IPipelineCodec,
    onEvent: (event: Event, parentId: String?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesList.none { it.message?.metadata?.protocol == codec.protocol }) {
                messageBatch.addGroups(messageGroup)
                continue
            }

            val parentEventId = messageGroup.parentEventId

            messageGroup.runCatching(codec::encode).onSuccess { encodedGroup ->
                if (encodedGroup.messagesCount > messageGroup.messagesCount) {
                    parentEventId.onEvent("Encoded message group contains more messages ($encodedGroup.messagesCount) than decoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(encodedGroup)
            }.onFailure {
                parentEventId.onEvent("Failed to encode message group", messageGroup.messageIds, it)
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }
}