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

import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec(
    private val messageRouter: MessageRouter<MessageGroupBatch>,
    private val eventRouter: MessageRouter<EventBatch>,
    private val processor: AbstractCodecProcessor,
    private val codecRootEvent: String
) : AutoCloseable, MessageListener<MessageGroupBatch> {
    private val logger = KotlinLogging.logger {}
    private var targetAttributes: String = ""

    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.targetAttributes = targetAttributes
            messageRouter.subscribeAll(this, sourceAttributes)
        } catch (exception: Exception) {
            when (exception) {
                is IOException,
                is TimeoutException -> throw DecodeException("could not start rabbit mq subscriber", exception)
                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {}

    override fun handler(consumerTag: String?, message: MessageGroupBatch) =
        messageRouter.sendAll(handleMessage(message), targetAttributes)

    fun handleMessage(message: MessageGroupBatch): MessageGroupBatch {
        var protoResult: MessageGroupBatch? = null

        try {
            protoResult = processor.process(message)
            if (!checkResult(protoResult)) throw CodecException("checkResult failed")
            return protoResult

        } catch (e: CodecException) {
            val parentEventId = getParentEventId(codecRootEvent, message, protoResult)
            createAndStoreErrorEvent(e, parentEventId)
            logger.error(e) { "Failed to handle message: ${message.toDebugString()}" }
            throw e
        }
    }

    private fun createAndStoreErrorEvent(exception: CodecException, parentEventID: String) {
        try {
            eventRouter.send(
                EventBatch.newBuilder().addEvents(
                    Event.start()
                        .name("Codec error")
                        .type("CodecError")
                        .status(FAILED)
                        .bodyData(Message().apply {
                            data = exception.getAllMessages()
                        })
                        .toProtoEvent(parentEventID)
                ).build()
            )
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    abstract fun getParentEventId(codecRootID: String, protoSource: MessageGroupBatch, protoResult: MessageGroupBatch?): String
    abstract fun checkResult(protoResult: MessageGroupBatch): Boolean
}