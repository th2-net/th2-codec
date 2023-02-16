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

import com.exactpro.th2.codec.configuration.ApplicationContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.storeEvent
import mu.KotlinLogging
import java.time.LocalDateTime

class Application(commonFactory: CommonFactory): AutoCloseable {
    private val configuration = Configuration.create(commonFactory)
    private val context = ApplicationContext.create(configuration, commonFactory)

    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val messageRouter: MessageRouter<MessageGroupBatch> = commonFactory.messageRouterMessageGroupBatch

    private val codecName = commonFactory.boxConfiguration?.boxName?.let { "$it " } ?: ""
    private val rootEventId: String = eventRouter.storeEvent(
        Event.start().apply {
            name("Codec $codecName[${LocalDateTime.now()}] protocols: ${context.protocols.joinToString(",")} ")
            type("CodecRoot")
        }
    ).id

    private val onEvent: (Event, String?) -> Unit = { event, parentId ->
        eventRouter.runCatching {
            storeEvent(event, parentId ?: rootEventId)
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

    private val decoder: AutoCloseable = SyncDecoder(
        messageRouter, eventRouter,
        DecodeProcessor(context.codec, context.protocols, true, configuration.enableVerticalScaling, onEvent),
        rootEventId
    ).apply {
        start(Configuration.DECODER_INPUT_ATTRIBUTE, Configuration.DECODER_OUTPUT_ATTRIBUTE)
    }

    private val encoder: AutoCloseable = SyncEncoder(
        messageRouter,
        eventRouter,
        EncodeProcessor(context.codec, context.protocols, true, configuration.enableVerticalScaling, onEvent),
        rootEventId
    ).apply {
        start(Configuration.ENCODER_INPUT_ATTRIBUTE, Configuration.ENCODER_OUTPUT_ATTRIBUTE)
    }

    private val generalDecoder: AutoCloseable = SyncDecoder(
            commonFactory.messageRouterMessageGroupBatch,
            commonFactory.eventBatchRouter,
            DecodeProcessor(context.codec, context.protocols, false, configuration.enableVerticalScaling, onRootEvent),
            rootEventId
        ).apply {
            start(Configuration.GENERAL_DECODER_INPUT_ATTRIBUTE, Configuration.GENERAL_DECODER_OUTPUT_ATTRIBUTE)
        }

    private val generalEncoder: AutoCloseable = SyncEncoder(
            commonFactory.messageRouterMessageGroupBatch,
            commonFactory.eventBatchRouter,
            EncodeProcessor(context.codec, context.protocols, false, configuration.enableVerticalScaling, onRootEvent),
            rootEventId
        ).apply {
            start(Configuration.GENERAL_ENCODER_INPUT_ATTRIBUTE, Configuration.GENERAL_ENCODER_OUTPUT_ATTRIBUTE)
        }

    init {
        K_LOGGER.info { "codec started" }
    }

    override fun close() {
        runCatching(generalEncoder::close).onFailure {
            K_LOGGER.error(it) { "General encoder closing failure" }
        }
        runCatching(generalDecoder::close).onFailure {
            K_LOGGER.error(it) { "General decoder closing failure" }
        }
        runCatching(encoder::close).onFailure {
            K_LOGGER.error(it) { "Encoder closing failure" }
        }
        runCatching(decoder::close).onFailure {
            K_LOGGER.error(it) { "Decoder closing failure" }
        }

        runCatching(context::close).onFailure {
            K_LOGGER.error(it) { "Application context closing failure" }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}