/*
 *  Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.event.Event.Status
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

    protected fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()): Event = null.onEvent(message, messagesIds)

    protected fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null): Event = null.onErrorEvent(message, messagesIds, cause)

    protected fun String?.onEvent(message: String, messagesIds: List<MessageID> = emptyList()): Event {
        logger.warn(message)
        val event = createEvent(message, messagesIds)
        onEvent(event, this)
        return event
    }

    protected fun String?.onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null): Event {
        logger.error(cause) { "$message. Messages: ${messagesIds.joinToString(", ") {
            "${it.connectionId.sessionAlias}:${it.direction}:${it.sequence}[.${it.subsequenceList.joinToString(".")}]"
        }}" }
        val event = createEvent(message, messagesIds, FAILED, cause)
        onEvent(event, this)
        return event
    }

    /**
     * Use only if eventId not needed
     */
    protected fun Set<String?>.forEachEvent(message: String, messagesIds: List<MessageID> = emptyList()) {
        when (isEmpty()) {
            true -> null.onEvent(message, messagesIds)
            false -> forEach {
                it.onEvent(message, messagesIds)
            }
        }
    }

    protected fun Set<String?>.forEachErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) {
        when (isEmpty()) {
            true -> null.onErrorEvent(message, messagesIds, cause)
            false -> map {
                it.onErrorEvent(message, messagesIds, cause)
            }
        }
    }

    protected fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
        incomingProtocols.none { it.isBlank() || it in this }  -> false
        incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
        else -> true
    }

    private fun createEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Status = PASSED,
        cause: Throwable? = null
    ) = Event.start().apply {
        name(message)
        type(if (status != PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) FAILED else status)
        messagesIds.forEach(::messageID)

        generateSequence(cause, Throwable::cause).forEach {
            bodyData(EventUtils.createMessageBean(it.message))
        }
    }
}