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

package com.exactpro.th2.codec.mq

import com.exactpro.th2.codec.CodecException
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeoutException

class MqListener<BATCH>(
    private val router: MessageRouter<BATCH>,
    private val handleBatch: (BATCH) -> BATCH,
    sourceAttributes: String,
    private val targetAttributes: String
) : AutoCloseable {

    private val subscriberMonitor: SubscriberMonitor = try {
        router.subscribeAll(::batchListener, sourceAttributes)
    } catch (exception: Exception) {
        when (exception) {
            is IOException,
            is TimeoutException,
            -> throw DecodeException("could not start rabbit mq subscriber", exception)

            else -> throw DecodeException("could not start decoder", exception)
        }
    }

    private fun batchListener(deliveryMetadata: DeliveryMetadata, batch: BATCH) {
        try {
            val result: BATCH? = handleBatch(batch)
            router.sendAll(result, this.targetAttributes)
        } catch (exception: CodecException) {
            LOGGER.error(exception) { "Failed to handle message: $batch" }
        }
    }

    override fun close() = subscriberMonitor.unsubscribe()

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}