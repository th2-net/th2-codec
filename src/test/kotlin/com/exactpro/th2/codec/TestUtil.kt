/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import java.time.Instant

const val BOOK_NAME = "test-book"
const val SESSION_GROUP_NAME = "test-session-group"

val CODEC_EVENT_ID: EventId = EventId(
    "test-codec",
    BOOK_NAME,
    "test-scope",
    Instant.now()
)

val MESSAGE_ID: MessageId = MessageId(
    "test-session-alias",
    Direction.INCOMING,
    1,
    Instant.now()
)

enum class Protocol { PROTO, TRANSPORT }

fun getNewBatchBuilder(protocol: Protocol, book: String, sessionGroup: String): IBatchBuilder = when(protocol) {
    Protocol.PROTO -> ProtoBatchBuilder(book, sessionGroup)
    Protocol.TRANSPORT -> TransportBatchBuilder(book, sessionGroup)
}

class UniversalCodecProcessor(
    codec: IPipelineCodec,
    protocols: Set<String>,
    useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    private val protocol: Protocol,
    process: AbstractCodecProcessor.Process,
    eventProcessor: EventProcessor,
    config: Configuration
) {
    private lateinit var protoProcessor: ProtoCodecProcessor
    private lateinit var transportProcessor: TransportCodecProcessor

    init {
        when(protocol) {
            Protocol.PROTO -> protoProcessor = ProtoCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config)
            Protocol.TRANSPORT -> transportProcessor = TransportCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config)
        }
    }

    fun process(batchWrapper: IGroupBatch): IGroupBatch = when(protocol) {
        Protocol.PROTO -> ProtoBatchWrapper(protoProcessor.process((batchWrapper as ProtoBatchWrapper).batch))
        Protocol.TRANSPORT -> TransportBatchWrapper(transportProcessor.process((batchWrapper as TransportBatchWrapper).batch))
    }
}