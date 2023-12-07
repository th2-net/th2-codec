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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventID
import mu.KotlinLogging

// TODO: merging AbstractCodec and AbstractCodecProcessor classes into one class should be considered
abstract class AbstractCodec<BATCH>(
    private val eventProcessor: EventProcessor
) {
    private val codecRootEvent: EventID = eventProcessor.codecEventID

    fun handleBatch(batch: BATCH): BATCH {
        var result: BATCH? = null

        try {
            result = process(batch)
            if (isEmpty(result)) throw CodecException("Result batch is empty")
            return result
        } catch (e: CodecException) {
            // FIXME: publish for each parent events (current: decoder publishes for root, encoder publishes for first parent event)
            val parentEventId = getParentEventId(codecRootEvent, batch, result)
            createAndStoreErrorEvent(e, parentEventId)
            throw e
        }
    }

    private fun createAndStoreErrorEvent(exception: CodecException, parentEventID: EventID) {
        try {
            eventProcessor.onEvent(
                Event.start()
                    .endTimestamp()
                    .name("Codec error")
                    .type("CodecError")
                    .status(FAILED)
                    .bodyData(Message().apply {
                        data = exception.getAllMessages()
                    }),
                parentEventID,
            )
        } catch (exception: Exception) {
            LOGGER.warn(exception) { "could not send codec error event" }
        }
    }

    protected abstract fun process(batch: BATCH): BATCH

    abstract fun getParentEventId(codecRootID: EventID, source: BATCH, result: BATCH?): EventID
    abstract fun isEmpty(result: BATCH): Boolean

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}