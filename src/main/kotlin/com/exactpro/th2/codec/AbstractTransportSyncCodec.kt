/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup

abstract class AbstractTransportSyncCodec(
    eventProcessor: EventProcessor,
    private val processor: AbstractCodecProcessor<GroupBatch, MessageGroup, Message<*>>,
) : AbstractCodec<GroupBatch>(eventProcessor) {
    override fun process(batch: GroupBatch): GroupBatch = processor.process(batch)
    override fun isEmpty(result: GroupBatch): Boolean = result.groups.isEmpty()
}