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
import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.configuration.TransportType
import com.exactpro.th2.codec.grpc.GrpcCodecService
import com.exactpro.th2.codec.mq.MqListener
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.storeEvent
import io.grpc.Server
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

class Application(commonFactory: CommonFactory): AutoCloseable {
    private val configuration = Configuration.create(commonFactory)
    private val context = ApplicationContext.create(configuration, commonFactory)

    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val protoRouter: MessageRouter<MessageGroupBatch> = commonFactory.messageRouterMessageGroupBatch
    private val transportRouter: MessageRouter<GroupBatch> = commonFactory.transportGroupBatchRouter
    private val rootEventId: EventID = commonFactory.rootEventId
    private val onEvent: (ProtoEvent) -> Unit = { event ->
        eventRouter.runCatching {
            sendAll(EventBatch.newBuilder().addEvents(event).build())
        }.onFailure {
            K_LOGGER.error(it) { "Failed to store event: $event" }
        }
    }

    private val onRootEvent: (Event, String?) -> Unit = { event, parentId ->
        if (parentId == null) {
            eventRouter.runCatching {
                storeEvent(event, rootEventId)
            }.onFailure {
                K_LOGGER.error(it) { "Failed to store event: $event" }
            }
        }
    }

    private val eventProcessor = EventProcessor(rootEventId, onEvent)

    private val codecs: MutableList<AutoCloseable> = mutableListOf<AutoCloseable>().apply {
        configuration.transportLines.forEach { (prefix, line) ->
            val prefixFull = if (prefix.isNotEmpty()) prefix + '_' else ""
            add(
                when (line.type) {
                    TransportType.PROTOBUF -> ::createMqProtoDecoder
                    TransportType.TH2_TRANSPORT -> ::createMqTransportDecoder
                }("${prefixFull}decoder", "${prefixFull}decoder_in", "${prefixFull}decoder_out", line.useParentEventId)
            )
            add(
                when (line.type) {
                    TransportType.PROTOBUF -> ::createMqProtoEncoder
                    TransportType.TH2_TRANSPORT -> ::createMqTransportEncoder
                }("${prefixFull}encoder", "${prefixFull}encoder_in", "${prefixFull}encoder_out", line.useParentEventId)
            )
        }
    }

    private val grpcServer: Server

    init {
        val grpcRouter: GrpcRouter = commonFactory.grpcRouter
        val grpcEncoder = createSyncCodec(::ProtoSyncEncoder, ::ProtoEncodeProcessor, true)
        val grpcDecoder = createSyncCodec(::ProtoSyncDecoder, ::ProtoDecodeProcessor, true)
        val grpcService = GrpcCodecService(grpcRouter, grpcDecoder::handleBatch, grpcEncoder::handleBatch, configuration.isFirstCodecInPipeline, eventProcessor)
        grpcServer = grpcRouter.startServer(grpcService)
        grpcServer.start()
        K_LOGGER.info { "codec started" }
    }

    private fun <CODEC : AbstractCodec<*>, PROCESSOR : AbstractCodecProcessor<*, *, *>> createSyncCodec(
        codecConstructor: (MessageRouter<EventBatch>, PROCESSOR, EventID) -> CODEC,
        processorConstructor: (IPipelineCodec, Set<String>, Boolean, Boolean, EventProcessor) -> PROCESSOR,
        useParentEventId: Boolean
    ) = codecConstructor(
        eventRouter,
        processorConstructor(context.codec, context.protocols, useParentEventId, configuration.enableVerticalScaling, eventProcessor),
        rootEventId
    )

    private fun createMqProtoEncoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
        useParentEventId: Boolean
    ): AutoCloseable = MqListener(
        protoRouter,
        createSyncCodec(::ProtoSyncEncoder, ::ProtoEncodeProcessor, useParentEventId)::handleBatch,
        sourceAttributes,
        targetAttributes
    ).also { K_LOGGER.info { "Proto '$codecName' started" } }

    private fun createMqProtoDecoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
        useParentEventId: Boolean
    ): AutoCloseable = MqListener(
        protoRouter,
        createSyncCodec(::ProtoSyncDecoder, ::ProtoDecodeProcessor, useParentEventId)::handleBatch,
        sourceAttributes,
        targetAttributes
    ).also { K_LOGGER.info { "Proto '$codecName' started" } }

    private fun createMqTransportEncoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
        useParentEventId: Boolean
    ): AutoCloseable = MqListener(
        transportRouter,
        createSyncCodec(::TransportSyncEncoder, ::TransportEncodeProcessor, useParentEventId)::handleBatch,
        sourceAttributes,
        targetAttributes
    ).also { K_LOGGER.info { "Transport '$codecName' started" } }

    private fun createMqTransportDecoder(
        codecName: String,
        sourceAttributes: String,
        targetAttributes: String,
        useParentEventId: Boolean
    ): AutoCloseable =  MqListener(
        transportRouter,
        createSyncCodec(::TransportSyncDecoder, ::TransportDecodeProcessor, useParentEventId)::handleBatch,
        sourceAttributes,
        targetAttributes
    ).also { K_LOGGER.info { "Transport '$codecName' started" } }

    override fun close() {
        if(!grpcServer.shutdown().awaitTermination(10, TimeUnit.SECONDS)) {
            K_LOGGER.warn { "gRPC server failed to shutdown orderly. Server will be stopped forcefully." }
            grpcServer.shutdownNow()
        }

        codecs.forEach { codec ->
            runCatching(codec::close).onFailure {
                K_LOGGER.error(it) { "Codec closing failure" }
            }
        }

        runCatching(context::close).onFailure {
            K_LOGGER.error(it) { "Application context closing failure" }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}