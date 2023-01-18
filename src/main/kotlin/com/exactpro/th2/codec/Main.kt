/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.concurrent.thread
import kotlin.system.exitProcess

typealias ProtoEvent = com.exactpro.th2.common.grpc.Event
private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configs: String? by option(help = "Directory containing schema files")
    private val codecSettings: String? by option(help = "Path to codec settings file")
    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

    private val commonFactory: CommonFactory = when (configs) {
        null -> CommonFactory()
        else -> CommonFactory.createFromArguments("--configs=$configs")
    }.apply {
        resources.add {
            logger.info("Closing common factory")
            close()
        }
    }

    private val configuration = Configuration.create(commonFactory, codecSettings)
    private val applicationContext = ApplicationContext.create(configuration, commonFactory).apply { resources += ::close }

    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter

    private val rootEventId = commonFactory.rootEventId

    private val onEvent: (ProtoEvent) -> Unit = { event ->
        eventRouter.runCatching {
            sendAll(EventBatch.newBuilder().addEvents(event).build())
        }.onFailure {
            logger.error(it) { "Failed to store event: $event" }
        }
    }

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
            val messageRouter = commonFactory.messageRouterMessageGroupBatch

            createCodec("decoder") {
                SyncDecoder(
                    messageRouter, eventRouter,
                    DecodeProcessor(applicationContext.codec, applicationContext.protocols, rootEventId, onEvent = onEvent),
                    rootEventId
                ).apply {
                    start(Configuration.DECODER_INPUT_ATTRIBUTE, Configuration.DECODER_OUTPUT_ATTRIBUTE)
                }
            }

            createCodec("encoder") {
                SyncEncoder(
                    messageRouter,
                    eventRouter,
                    EncodeProcessor(applicationContext.codec, applicationContext.protocols, rootEventId, onEvent = onEvent),
                    rootEventId
                ).apply {
                    start(Configuration.ENCODER_INPUT_ATTRIBUTE, Configuration.ENCODER_OUTPUT_ATTRIBUTE)
                }
            }

            createGeneralDecoder(applicationContext, rootEventId)
            createGeneralEncoder(applicationContext, rootEventId)

            logger.info { "codec started" }
        } catch (exception: Exception) {
            logger.error(exception) { "fatal error. Exit the program" }
            exitProcess(1)
        }
    }

    private fun createGeneralEncoder(
        context: ApplicationContext,
        rootEventId: EventID
    ) {
        val commonFactory = context.commonFactory

        createCodec("general-encoder") {
            SyncEncoder(
                commonFactory.messageRouterMessageGroupBatch,
                commonFactory.eventBatchRouter,
                EncodeProcessor(context.codec, context.protocols, rootEventId, false, onEvent),
                rootEventId
            ).apply {
                start(Configuration.GENERAL_ENCODER_INPUT_ATTRIBUTE, Configuration.GENERAL_ENCODER_OUTPUT_ATTRIBUTE)
            }
        }
    }

    private fun createGeneralDecoder(
        context: ApplicationContext,
        rootEventId: EventID
    ) {
        val commonFactory = context.commonFactory

        createCodec("general-decoder") {
            SyncDecoder(
                commonFactory.messageRouterMessageGroupBatch,
                commonFactory.eventBatchRouter,
                DecodeProcessor(context.codec, context.protocols, rootEventId, false, onEvent),
                rootEventId
            ).apply {
                start(Configuration.GENERAL_DECODER_INPUT_ATTRIBUTE, Configuration.GENERAL_DECODER_OUTPUT_ATTRIBUTE)
            }
        }
    }

    private inline fun createCodec(codecName: String, codecFactory: () -> AutoCloseable) {
        codecFactory().apply {
            resources.add {
                logger.info { "Closing '$codecName' codec" }
                close()
            }
        }

        logger.info { "'$codecName' started" }
    }
}



