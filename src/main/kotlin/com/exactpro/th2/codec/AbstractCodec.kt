/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeoutException

abstract class AbstractCodec<BATCH>(
    private val router: MessageRouter<BATCH>,
    private val eventRouter: MessageRouter<EventBatch>,
    private val codecRootEvent: EventID
) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private var targetAttributes: String = ""

    fun start(sourceAttributes: String, targetAttributes: String) {
        try {
            this.targetAttributes = targetAttributes
            router.subscribeAll(::handle, sourceAttributes)
        } catch (exception: Exception) {
            when (exception) {
                is IOException,
                is TimeoutException,
                -> throw DecodeException("could not start rabbit mq subscriber", exception)

                else -> throw DecodeException("could not start decoder", exception)
            }
        }
    }

    override fun close() {}

    private fun handle(deliveryMetadata: DeliveryMetadata, batch: BATCH) {
        var result: BATCH? = null

        try {
            result = process(batch)

            if (checkResult(result)) {
                router.sendAll(result, this.targetAttributes)
            }
        } catch (exception: CodecException) {
            val parentEventId = getParentEventId(codecRootEvent, batch, result)
            createAndStoreErrorEvent(exception, parentEventId)
            logger.error(exception) { "Failed to handle message: $batch" }
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

    protected abstract fun process(batch: BATCH): BATCH

    abstract fun getParentEventId(codecRootID: EventID, source: BATCH, result: BATCH?): EventID
    abstract fun checkResult(result: BATCH): Boolean
}