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

import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeoutException

abstract class AbstractSyncCodec(
    private val messageRouter: MessageRouter<MessageGroupBatch>,
    private val eventRouter: MessageRouter<EventBatch>,
    private val processor: AbstractCodecProcessor,
    private val codecRootEvent: EventID
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

    override fun handle(deliveryMetadata: DeliveryMetadata, message: MessageGroupBatch) {
        var protoResult: MessageGroupBatch? = null

        try {
            protoResult = processor.process(message)

            if (checkResult(protoResult)) {
                messageRouter.sendAll(protoResult, this.targetAttributes)
            }
        } catch (exception: CodecException) {
            val parentEventId = getParentEventId(codecRootEvent, message, protoResult)
            createAndStoreErrorEvent(exception, parentEventId)
            logger.error(exception) { "Failed to handle message: ${message.toDebugString()}" }
        }
    }

    private fun createAndStoreErrorEvent(exception: CodecException, parentEventID: EventID) {
        try {
            eventRouter.send(
                EventBatch.newBuilder().addEvents(
                    Event.start()
                        .name("Codec error")
                        .type("CodecError")
                        .status(FAILED)
                        .bodyData(Message().apply {
                            data = exception.getAllMessages()
                        }).toProto(parentEventID)
                ).build()
            )
        } catch (exception: Exception) {
            logger.warn(exception) { "could not send codec error event" }
        }
    }

    abstract fun getParentEventId(codecRootID: EventID, protoSource: MessageGroupBatch, protoResult: MessageGroupBatch?): EventID
    abstract fun checkResult(protoResult: MessageGroupBatch): Boolean
}