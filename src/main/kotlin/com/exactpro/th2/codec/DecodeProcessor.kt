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
import com.exactpro.th2.codec.util.parentEventId
import com.exactpro.th2.codec.util.toErrorMessageGroup
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch

class DecodeProcessor(
    codec: IPipelineCodec,
    private val protocol: String,
    onEvent: (event: Event, parentId: EventID?) -> Unit,
    private val boxBookName: String,
) : AbstractCodecProcessor(codec, onEvent) {

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            val parentEventId = messageGroup.parentEventId
            val bookName = parentEventId?.bookName ?: boxBookName
            messageGroup.runCatching(codec::decode).onSuccess { decodedGroup ->
                if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                    parentEventId.onEvent(bookName, "Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(decodedGroup)
            }.onFailure {
                parentEventId.onErrorEvent(bookName, "Failed to decode message group", messageGroup.messageIds, it)
                messageBatch.addGroups(messageGroup.toErrorMessageGroup(it, protocol))
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent(boxBookName, "Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }
}