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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.codec.util.allParsedProtocols
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.grpc.MessageID as ProtoMessageID

open class TransportCodecProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    process: Process,
    eventProcessor: EventProcessor,
    config: Configuration
) : AbstractCodecProcessor<GroupBatch, MessageGroup, Message<*>>(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config) {
    override val GroupBatch.batchItems: List<MessageGroup> get() = groups
    override val MessageGroup.size: Int get() = messages.size
    override val MessageGroup.groupItems: List<Message<*>> get() = messages
    override val Message<*>.isRaw: Boolean get() = this is RawMessage
    override val Message<*>.isParsed: Boolean get() = this is ParsedMessage
    override val MessageGroup.rawProtocols get() = allRawProtocols
    override val MessageGroup.parsedProtocols get() = allParsedProtocols
    override val MessageGroup.eventIds get() = allParentEventIds
    override fun MessageGroup.ids(batch: GroupBatch): List<ProtoMessageID> = messageIds(batch)
    override val toErrorGroup get() = MessageGroup::toErrorGroup
    override fun MessageGroup.toReadableBody(): List<String> = messages.map(Message<*>::toJson)
    override fun createBatch(sourceBatch: GroupBatch, groups: List<MessageGroup>) = GroupBatch(sourceBatch.book, sourceBatch.sessionGroup, groups)
    override fun IPipelineCodec.genericDecode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.decode(group, context)
    override fun IPipelineCodec.genericEncode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.encode(group, context)
}

class TransportDecodeProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    eventProcessor: EventProcessor,
    config: Configuration
) : TransportCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, Process.DECODE, eventProcessor, config)

class TransportEncodeProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    eventProcessor: EventProcessor,
    config: Configuration
) : TransportCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, Process.ENCODE, eventProcessor, config)