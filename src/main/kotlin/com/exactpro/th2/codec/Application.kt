/*
 *  Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.shutdownGracefully
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.grpc.Server
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class Application(commonFactory: CommonFactory): AutoCloseable {
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("event-batcher-%d")
            .build()
    )
    private val configuration = Configuration.create(commonFactory)
    private val context = ApplicationContext.create(configuration, commonFactory)

    private val protoRouter: MessageRouter<MessageGroupBatch> = commonFactory.messageRouterMessageGroupBatch
    private val transportRouter: MessageRouter<GroupBatch> = commonFactory.transportGroupBatchRouter
    private val eventBatcher: EventBatcher = run {
        val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
        val eventPublication = configuration.eventPublication
        EventBatcher(
            maxFlushTime = eventPublication.flushTimeout,
            maxBatchSizeInItems = eventPublication.batchSize,
            // TODO: it should be simpler to get the max batch size
            maxBatchSizeInBytes = commonFactory.cradleMaxEventBatchSize,
            executor = executor,
        ) { batch ->
            eventRouter.runCatching {
                sendAll(batch)
            }.onFailure {
                K_LOGGER.error(it) { "Failed to store event batch: ${batch.toJson()}" }
            }
        }
    }

    private val eventProcessor = EventProcessor(
        commonFactory.boxConfiguration.bookName,
        commonFactory.boxConfiguration.boxName,
        eventBatcher::onEvent
    )

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
        codecConstructor: (EventProcessor, PROCESSOR) -> CODEC,
        processorConstructor: (IPipelineCodec, Set<String>, Boolean, Boolean, EventProcessor, Configuration) -> PROCESSOR,
        useParentEventId: Boolean
    ) = codecConstructor(
        eventProcessor,
        processorConstructor(context.codec, context.protocols, useParentEventId, configuration.enableVerticalScaling, eventProcessor, configuration),
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

        runCatching(eventBatcher::close).onFailure {
            K_LOGGER.error(it) { "cannot close event batcher" }
        }

        runCatching { executor.shutdownGracefully(timeout = 10, unit = TimeUnit.SECONDS) }.onFailure {
            K_LOGGER.error(it) { "cannot shutdown event batcher's executor service " }
        }

        runCatching(context::close).onFailure {
            K_LOGGER.error(it) { "Application context closing failure" }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}