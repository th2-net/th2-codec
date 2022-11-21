/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter

class SyncDecoder(
    messageRouter: MessageRouter<MessageGroupBatch>,
    eventRouter: MessageRouter<EventBatch>,
    processor: AbstractCodecProcessor,
    codecRootID: String,
    enabledExternalQueueRouting: Boolean
) : AbstractSyncCodec(
    messageRouter,
    eventRouter,
    processor,
    codecRootID,
    enabledExternalQueueRouting
) {
    override fun getParentEventId(codecRootID: String, protoSource: MessageGroupBatch, protoResult: MessageGroupBatch?): String = codecRootID
    override fun checkResult(protoResult: MessageGroupBatch): Boolean = protoResult.groupsCount != 0
    override fun isCompletelyProcessed(protoResult: MessageGroupBatch): Boolean = protoResult.groupsList.all { it.messagesList.all(AnyMessage::hasMessage) }
}