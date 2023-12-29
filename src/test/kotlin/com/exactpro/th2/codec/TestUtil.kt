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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import strikt.api.Assertion
import strikt.assertions.contains
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import java.time.Instant

const val BOOK_NAME_A = "test-book-a"
const val BOOK_NAME_B = "test-book-b"
const val BOOK_NAME_C = "test-book-c"
const val SESSION_ALIAS_NAME = "test-session-alias"
const val SESSION_GROUP_NAME = "test-session-group"
const val COMPONENT_NAME = "test-component"
const val EVENT_ID = "test-codec"
const val EVENT_SCOPE = COMPONENT_NAME

val CODEC_EVENT_ID_BOOK_A: EventId = EventId(
    EVENT_ID,
    BOOK_NAME_A,
    EVENT_SCOPE,
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

class UniversalCodec(
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

    private lateinit var protoDecoder: ProtoSyncDecoder
    private lateinit var protoEncoder: ProtoSyncEncoder

    private lateinit var transportDecoder: TransportSyncDecoder
    private lateinit var transportEncoder: TransportSyncEncoder

    val codecEventId: EventID = eventProcessor.codecEventID


    init {
        when(protocol) {
            Protocol.PROTO -> {
                protoProcessor = ProtoCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config)
                protoDecoder = ProtoSyncDecoder(
                    eventProcessor,
                    ProtoDecodeProcessor(
                        codec,
                        protocols,
                        useParentEventId,
                        enabledVerticalScaling,
                        eventProcessor,
                        config
                    )
                )
                protoEncoder = ProtoSyncEncoder(
                    eventProcessor,
                    ProtoEncodeProcessor(
                        codec,
                        protocols,
                        useParentEventId,
                        enabledVerticalScaling,
                        eventProcessor,
                        config
                    )
                )
            }
            Protocol.TRANSPORT -> {
                transportProcessor = TransportCodecProcessor(codec, protocols, useParentEventId, enabledVerticalScaling, process, eventProcessor, config)
                transportDecoder = TransportSyncDecoder(
                    eventProcessor,
                    TransportDecodeProcessor(
                        codec,
                        protocols,
                        useParentEventId,
                        enabledVerticalScaling,
                        eventProcessor,
                        config
                    )
                )
                transportEncoder = TransportSyncEncoder(
                    eventProcessor,
                    TransportEncodeProcessor(
                        codec,
                        protocols,
                        useParentEventId,
                        enabledVerticalScaling,
                        eventProcessor,
                        config
                    )
                )

            }
        }
    }

    fun process(batchWrapper: IGroupBatch): IGroupBatch = when(protocol) {
        Protocol.PROTO -> ProtoBatchWrapper(protoProcessor.process((batchWrapper as ProtoBatchWrapper).batch))
        Protocol.TRANSPORT -> TransportBatchWrapper(transportProcessor.process((batchWrapper as TransportBatchWrapper).batch))
    }

    fun encode(batchWrapper: IGroupBatch): IGroupBatch = when(protocol) {
        Protocol.PROTO -> ProtoBatchWrapper(protoEncoder.handleBatch((batchWrapper as ProtoBatchWrapper).batch))
        Protocol.TRANSPORT -> TransportBatchWrapper(transportEncoder.handleBatch((batchWrapper as TransportBatchWrapper).batch))
    }

    fun decode(batchWrapper: IGroupBatch): IGroupBatch = when(protocol) {
        Protocol.PROTO -> ProtoBatchWrapper(protoDecoder.handleBatch((batchWrapper as ProtoBatchWrapper).batch))
        Protocol.TRANSPORT -> TransportBatchWrapper(transportDecoder.handleBatch((batchWrapper as TransportBatchWrapper).batch))
    }
}

fun Assertion.Builder<ProtoEvent>.isRootEvent(book: String, scope: String) {
    get { id }.apply {
        get { bookName }.isEqualTo(book)
        get { scope }.isEqualTo(scope)
    }
    get { hasParentId() }.isFalse()
    get { name }.contains("Root event")
        .contains(scope)
    get { type }.isEqualTo("Microservice")
    get { status }.isEqualTo(EventStatus.SUCCESS)
}