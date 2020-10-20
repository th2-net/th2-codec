/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.schema.factory.CommonFactory
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.time.LocalDateTime

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    CodecCommand().main(args)
}

class CodecCommand : CliktCommand() {
    private val configs: String? by option(help = "Directory containing schema files")
    private val sailfishCodecConfig: String? by option(help = "Path to sailfish codec configuration file")
    override fun run() = runBlocking {
        try {
            runProgram(configs, sailfishCodecConfig)
        } catch (exception: Exception) {
            logger.error(exception) { "fatal error. Exit the program" }
        }
    }

    @ObsoleteCoroutinesApi
    private fun runProgram(configs: String?, sailfishCodecParamsPath: String?) {
        var commonFactory =  if (configs == null) //FIXME:Close at the end of programm
            CommonFactory()
         else
            CommonFactory.createFromArguments("--configs=" + configs)

        val configuration = Configuration.create(commonFactory, sailfishCodecParamsPath)

        val applicationContext = ApplicationContext.create(configuration, commonFactory)
        var parsedRouter= applicationContext.commonFactory.messageRouterParsedBatch
        var rawRouter = applicationContext.commonFactory.messageRouterRawBatch

        val rootEventId = createAndStoreRootEvent(applicationContext)

        createAndStartCodec("decoder", applicationContext, rootEventId)
        {  _: ApplicationContext, _: EventID? ->
            SyncDecoder(rawRouter, parsedRouter, applicationContext,
                DecodeProcessor(
                    applicationContext.codecFactory,
                    applicationContext.codecSettings,
                    applicationContext.messageToProtoConverter
                ),
                rootEventId).also { it.start(configuration.decoderInputAttribute, configuration.decoderOutputAttribute) }
        }

        createAndStartCodec("encoder", applicationContext, rootEventId)
        { _: ApplicationContext, _: EventID? ->
            SyncEncoder(parsedRouter, rawRouter, applicationContext,
                EncodeProcessor(
                    applicationContext.codecFactory,
                    applicationContext.codecSettings,
                    applicationContext.protoToIMessageConverter
                ),
                rootEventId).also { it.start(configuration.encoderInputAttribute, configuration.encoderOutputAttribute) }
        }

        createGeneralDecoder(applicationContext, configuration, rootEventId)
        createGeneralEncoder(applicationContext, configuration, rootEventId)
        logger.info { "codec started" }
    }

    private fun createGeneralEncoder(context: ApplicationContext, configuration: Configuration, rootEventId: EventID?) {
        createAndStartCodec ("general-encoder", context, rootEventId
        )
        { _: ApplicationContext, _: EventID? ->
            var parsedRouter= context.commonFactory.messageRouterParsedBatch
            var rawRouter = context.commonFactory.messageRouterRawBatch
            SyncEncoder(
                parsedRouter, rawRouter, context,
                EncodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.protoToIMessageConverter
                ),
                rootEventId
            ).also { it.start(configuration.generalEncoderInputAttribute, configuration.generalEncoderOutputAttribute) }
        }
    }

    private fun createGeneralDecoder(context: ApplicationContext, configuration: Configuration, rootEventId: EventID?) {
        createAndStartCodec ("general-decoder", context, rootEventId
        )
        {  _: ApplicationContext, _: EventID? ->
            var parsedRouter= context.commonFactory.messageRouterParsedBatch
            var rawRouter = context.commonFactory.messageRouterRawBatch
            SyncDecoder(
                rawRouter, parsedRouter, context,
                DecodeProcessor(
                    context.codecFactory,
                    context.codecSettings,
                    context.messageToProtoConverter
                ),
                rootEventId
            ).also { it.start(configuration.generalDecoderInputAttribute, configuration.generalDecoderOutputAttribute) }
        }
    }

    private fun createAndStartCodec(
        codecName: String,
        applicationContext: ApplicationContext,
        rootEventId: EventID?,
        creationFunction: (ApplicationContext, EventID?) -> AutoCloseable
    ) {
        val codecInstance = creationFunction.invoke(applicationContext, rootEventId)
        logger.info { "'$codecName' started" }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                try {
                    logger.info { "shutting down '$codecName'..." }
                    codecInstance.close()
                    logger.info { "'$codecName' closed successfully" }
                } catch (exception: Exception) {
                    logger.error(exception) { "Error upon '$codecName' closing" }
                }
            }
        })
    }


    private fun createAndStoreRootEvent(applicationContext: ApplicationContext): EventID? {
        val eventBatchRouter = applicationContext.eventBatchRouter
        if (eventBatchRouter != null) {
            try {
                var event = Event.start()
                    .name("Codec_${applicationContext.codec::class.java.simpleName}_${LocalDateTime.now()}")
                    .type("CodecRoot")
                    .toProtoEvent(null)

                eventBatchRouter.send(
                    EventBatch.newBuilder()
                        .addEvents(event)
                        .build(),
                    "publish", "event"
                )
                logger.info("stored root event, {}", event.id)
                return event.id
            } catch (exception: Exception) {
                logger.warn(exception) { "could not store root event" }
            }
        }
        return null
    }
}



