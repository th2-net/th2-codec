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
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.codec.util.allParsedProtocols
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.EventID

open class ProtoCodecProcessor (
    codec: IPipelineCodec,
    protocols: Set<String>,
    codecEventID: EventID,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    process: Process,
    onEvent: (event: ProtoEvent) -> Unit
) : AbstractCodecProcessor<MessageGroupBatch, MessageGroup, AnyMessage>(codec, protocols, codecEventID, useParentEventId, enabledVerticalScaling, process, onEvent) {
    override val MessageGroupBatch.batchItems: List<MessageGroup> get() = groupsList
    override val MessageGroup.size: Int get() = messagesCount
    override val MessageGroup.groupItems: List<AnyMessage> get() = messagesList
    override val AnyMessage.isRaw: Boolean get() = hasRawMessage()
    override val AnyMessage.isParsed: Boolean get() = hasMessage()
    override val MessageGroup.rawProtocols get() = allRawProtocols
    override val MessageGroup.parsedProtocols get() = allParsedProtocols
    override val MessageGroup.eventIds get() = allParentEventIds
    override fun MessageGroup.ids(batch: MessageGroupBatch): List<MessageID> = messageIds
    override val toErrorGroup get() = MessageGroup::toErrorGroup
    override fun MessageGroup.toReadableBody(): List<String> = messagesList.map(AnyMessage::toString)
    override fun createBatch(sourceBatch: MessageGroupBatch, groups: List<MessageGroup>): MessageGroupBatch = MessageGroupBatch.newBuilder().addAllGroups(groups).build()
    override fun IPipelineCodec.genericDecode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.decode(group, context)
    override fun IPipelineCodec.genericEncode(group: MessageGroup, context: ReportingContext): MessageGroup = codec.encode(group, context)
}

class ProtoDecodeProcessor(codec: IPipelineCodec,
                           protocols: Set<String>,
                           codecEventID: EventID,
                           useParentEventId: Boolean = true,
                           enabledVerticalScaling: Boolean = false,
                           onEvent: (event: ProtoEvent) -> Unit
) : ProtoCodecProcessor(codec, protocols, codecEventID, useParentEventId, enabledVerticalScaling, Process.DECODE, onEvent)

class ProtoEncodeProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    codecEventID: EventID,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    onEvent: (event: ProtoEvent) -> Unit
) : ProtoCodecProcessor(codec, protocols, codecEventID, useParentEventId, enabledVerticalScaling, Process.ENCODE, onEvent)