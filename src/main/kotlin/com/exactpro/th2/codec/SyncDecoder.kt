/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.RawMessageBatch
import com.exactpro.th2.schema.message.MessageRouter

class SyncDecoder(
    sourceRouter: MessageRouter<out RawMessageBatch>,
    targetRouter: MessageRouter<out MessageBatch>,
    applicationContext: ApplicationContext,
    processor: AbstractCodecProcessor<RawMessageBatch, MessageBatch>,
    codecRootID: EventID?
): AbstractSyncCodec< RawMessageBatch,  MessageBatch>(
    sourceRouter,
    targetRouter,
    applicationContext,
    processor,
    codecRootID
) {
    override fun getParentEventId(
        codecRootID: EventID?,
        protoSource: RawMessageBatch?,
        protoResult: MessageBatch?
    ): EventID? {
        return codecRootID
    }

    override fun parseProtoSourceFrom(data: ByteArray): RawMessageBatch = RawMessageBatch.parseFrom(data)

    override fun checkResult(protoResult: MessageBatch): Boolean = protoResult.messagesCount != 0

}