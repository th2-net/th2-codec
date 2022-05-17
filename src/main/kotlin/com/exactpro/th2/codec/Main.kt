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

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.grpc.GrpcCodecService
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.storeEvent
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import io.grpc.Server
import mu.KotlinLogging
import java.time.LocalDateTime
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configs: String? by option(help = "Directory containing schema files")
    private val codecSettings: String? by option(help = "Path to codec settings file")
    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

    init {
        Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown") {
                try {
                    logger.info { "Shutdown start" }
                    resources.descendingIterator().forEach { action ->
                        runCatching(action).onFailure { logger.error(it.message, it) }
                    }
                } finally {
                    logger.info { "Shutdown end" }
                }
        })
    }

    override fun run() {
        try {
            val commonFactory = when (configs) {
                null -> CommonFactory()
                else -> CommonFactory.createFromArguments("--configs=$configs")
            }.apply {
                resources.add {
                    logger.info("Closing common factory")
                    close()
                }
            }

            val configuration = Configuration.create(commonFactory, codecSettings)
            val applicationContext = ApplicationContext.create(configuration, commonFactory).apply { resources += ::close }
            val messageRouter = commonFactory.messageRouterMessageGroupBatch
            val eventRouter = commonFactory.eventBatchRouter

            val codecName = commonFactory.boxConfiguration?.boxName?.let { "$it " } ?: ""
            val rootEventId = eventRouter.storeEvent(
                Event.start().apply {
                    name("Codec $codecName[${LocalDateTime.now()}] protocols: ${applicationContext.protocols.joinToString(",")} ")
                    type("CodecRoot")
                }
            ).id

            val onEvent: (Event, String?) -> Unit = { event, parentId ->
                eventRouter.runCatching {
                    storeEvent(event, parentId ?: rootEventId)
                }.onFailure {
                    logger.error(it) { "Failed to store event: $event" }
                }
            }

            val eventProcessor = EventProcessor(onEvent)

            createCodec("decoder") {
                SyncDecoder(
                    messageRouter, eventRouter,
                    DecodeProcessor(applicationContext.codec, eventProcessor, applicationContext.protocols),
                    rootEventId
                ).apply {
                    start(Configuration.DECODER_INPUT_ATTRIBUTE, Configuration.DECODER_OUTPUT_ATTRIBUTE)
                }
            }

            createCodec("encoder") {
                SyncEncoder(
                    messageRouter,
                    eventRouter,
                    EncodeProcessor(applicationContext.codec, eventProcessor, applicationContext.protocols),
                    rootEventId
                ).apply {
                    start(Configuration.ENCODER_INPUT_ATTRIBUTE, Configuration.ENCODER_OUTPUT_ATTRIBUTE)
                }
            }

            val eventProcessorNoEventStore = EventProcessor(null)
            val decodeHandler = createGeneralDecoder(applicationContext, rootEventId, eventProcessorNoEventStore)::handleMessage
            val encodeHandler = createGeneralEncoder(applicationContext, rootEventId, eventProcessorNoEventStore)::handleMessage

            logger.info { "MQ codec service started" }

            val grpcRouter: GrpcRouter = commonFactory.grpcRouter
            val grpcService = GrpcCodecService(grpcRouter, decodeHandler, encodeHandler, onEvent)
            val server: Server = grpcRouter.startServer(grpcService)
            server.start()
            logger.info { "gRPC codec service started on port ${server.port}" }

        } catch (exception: Exception) {
            logger.error(exception) { "fatal error. Exit the program" }
            exitProcess(1)
        }
    }

    private fun createGeneralEncoder(
        context: ApplicationContext,
        rootEventId: String,
        eventProcessor: EventProcessor
    ) = createCodec("general-encoder") {
            SyncEncoder(
                context.commonFactory.messageRouterMessageGroupBatch,
                context.commonFactory.eventBatchRouter,
                EncodeProcessor(context.codec, eventProcessor, context.protocols, false),
                rootEventId
            ).apply {
                start(Configuration.GENERAL_ENCODER_INPUT_ATTRIBUTE, Configuration.GENERAL_ENCODER_OUTPUT_ATTRIBUTE)
            }
        }

    private fun createGeneralDecoder(
        context: ApplicationContext,
        rootEventId: String,
        eventProcessor: EventProcessor
    ) = createCodec("general-decoder") {
        SyncDecoder(
            context.commonFactory.messageRouterMessageGroupBatch,
            context.commonFactory.eventBatchRouter,
            DecodeProcessor(context.codec, eventProcessor, context.protocols,false),
            rootEventId
        ).apply {
            start(Configuration.GENERAL_DECODER_INPUT_ATTRIBUTE, Configuration.GENERAL_DECODER_OUTPUT_ATTRIBUTE)
        }
    }

    private inline fun createCodec(codecName: String, codecFactory: () -> AbstractSyncCodec) =
        codecFactory().apply {
            resources.add {
                logger.info { "Closing '$codecName' codec" }
                close()
            }

            logger.info { "'$codecName' started" }
        }
}