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
import com.exactpro.th2.codec.util.id
import com.exactpro.th2.codec.util.parentEventId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toJson

open class ProtoCodecProcessor (
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    process: Process,
    eventProcessor: EventProcessor,
    config: Configuration
) : AbstractCodecProcessor<MessageGroupBatch, MessageGroup, AnyMessage>(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config) {
    override val MessageGroupBatch.batchItems: List<MessageGroup> get() = groupsList
    override val MessageGroup.size: Int get() = messagesCount
    override val MessageGroup.groupItems: List<AnyMessage> get() = messagesList
    override val AnyMessage.isRaw: Boolean get() = hasRawMessage()
    override val AnyMessage.isParsed: Boolean get() = hasMessage()
    override val MessageGroup.rawProtocols get() = allRawProtocols
    override val MessageGroup.parsedProtocols get() = allParsedProtocols
    override val MessageGroup.eventIds get() = allParentEventIds
    override fun MessageGroup.ids(batch: MessageGroupBatch): List<MessageID> = messageIds
    override fun AnyMessage.id(batch: MessageGroupBatch): MessageID = id
    override fun AnyMessage.book(batch: MessageGroupBatch): String = id.bookName
    override val AnyMessage.eventBook: String? get() = parentEventId?.bookName
    override val AnyMessage.eventId: EventID? get() = parentEventId
    override val toErrorGroup get() = MessageGroup::toErrorGroup
    override fun MessageGroup.toReadableBody(): List<String> = messagesList.map(AnyMessage::toJson)
    override fun createBatch(sourceBatch: MessageGroupBatch, groups: List<MessageGroup>): MessageGroupBatch = MessageGroupBatch.newBuilder().addAllGroups(groups).build()
    override fun IPipelineCodec.genericDecode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.decode(group, context)
    override fun IPipelineCodec.genericEncode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.encode(group, context)
}

class ProtoDecodeProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    eventProcessor: EventProcessor,
    config: Configuration
) : ProtoCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, Process.DECODE, eventProcessor, config)

class ProtoEncodeProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    eventProcessor: EventProcessor,
    config: Configuration
) : ProtoCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, Process.ENCODE, eventProcessor, config)