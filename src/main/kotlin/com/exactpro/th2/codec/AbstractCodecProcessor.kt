/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import mu.KotlinLogging

abstract class AbstractCodecProcessor(
    protected val codec: IPipelineCodec,
    private val onEvent: (event: Event, parentId: String?) -> Unit
) : MessageProcessor<MessageGroupBatch, MessageGroupBatch> {
    private val logger = KotlinLogging.logger {}

    protected fun onEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = null.onEvent(message, messagesIds, cause)

    protected fun String?.onEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) {
        cause?.run { logger.error(message, this) } ?: logger.warn(message)
        onEvent(createEvent(message, messagesIds, cause), this)
    }

    private fun createEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = Event.start().apply {
        name(message)
        type(if (cause != null) "Error" else "Warn")
        status(if (cause != null) FAILED else PASSED)
        messagesIds.forEach { messageID(it) }

        generateSequence(cause, Throwable::cause).forEach {
            bodyData(EventUtils.createMessageBean(it.message))
        }
    }
}