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
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

class DecodeProcessor(
    codec: IPipelineCodec,
    onEvent: (name: String, type: String, cause: Throwable?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {
    private val logger = KotlinLogging.logger { }

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            messageGroup.runCatching(codec::decode).onSuccess {
                if (it.messagesCount < messageGroup.messagesCount) {
                    onEvent("Decoded message group contains less messages ($it.messagesCount) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(it)
            }.onFailure {
                onEvent("Failed to decode message group: ${messageGroup.toDebugString()}", it)
            }
        }

        return messageBatch.build().apply {
            if (source.groupsCount > groupsCount) {
                onEvent("Size out the output batch ($groupsCount) is smaller than of the input one (${source.groupsCount})")
            }
        }
    }
}