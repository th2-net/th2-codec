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
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import mu.KotlinLogging

abstract class AbstractCodecProcessor(
    protected val codec: IPipelineCodec,
    private val onEvent: (event: Event, parentId: String?) -> Unit
) : MessageProcessor<MessageGroupBatch, MessageGroupBatch> {
    private val logger = KotlinLogging.logger {}

    protected fun onEvent(message: String, messagesIds: List<MessageID> = emptyList()) = null.onEvent(message, messagesIds)

    protected fun onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null) = null.onErrorEvent(message, messagesIds, cause)

    protected fun String?.onEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) : String {
        logger.warn(message)
        val event = createEvent(message, messagesIds, body = body)
        onEvent(event, this)
        return event.id
    }

    protected fun String?.onErrorEvent(message: String, messagesIds: List<MessageID> = emptyList(), cause: Throwable? = null): String {
        logger.error(cause) { "$message. Messages: ${messagesIds.joinToString(", ") {
            "${it.connectionId.sessionAlias}:${it.direction}:${it.sequence}[.${it.subsequenceList.joinToString(".")}]"
        }}" }
        val event = createEvent(message, messagesIds, FAILED, cause)
        onEvent(event, this)
        return event.id
    }

    protected fun Set<String>.onEachEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        body: List<String> = emptyList()
    ) {
        val warnEvent = null.onEvent(message, messagesIds, body)
        forEach {
            it.addReferenceTo(warnEvent, message, PASSED)
        }
    }

    protected fun Set<String>.onEachErrorEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        cause: Throwable? = null
    ) {
        val errorEventId = null.onErrorEvent(message, messagesIds, cause)
        forEach {
            it.addReferenceTo(errorEventId, message, FAILED)
        }
    }

    protected fun Set<String>.onEachWarning(context: ReportingContext, action: String, messagesIds: () -> List<MessageID> = { emptyList() }) = context.warnings.let { warnings ->
        warnings.forEach { warning ->
            this.onEachEvent("[WARNING] Message $action produced ${warnings.size} warnings", messagesIds(), body = listOf(warning))
        }
    }

    private fun String.addReferenceTo(eventId: String, name: String, status: Status) {
        onEvent(
            Event
                .start()
                .endTimestamp()
                .name(name)
                .type(if (status != PASSED) "Error" else "Warn")
                .bodyData(EventUtils.createMessageBean("This event contains reference to the codec event"))
                .bodyData(ReferenceToEvent(eventId)),
            this
        )
    }

    private fun createEvent(
        message: String,
        messagesIds: List<MessageID> = emptyList(),
        status: Status = PASSED,
        cause: Throwable? = null,
        body: List<String> = emptyList(),
    ) = Event.start().apply {
        name(message)
        type(if (status != PASSED || cause != null) "Error" else "Warn")
        status(if (cause != null) FAILED else status)
        messagesIds.forEach(::messageID)

        generateSequence(cause, Throwable::cause).forEach {
            bodyData(EventUtils.createMessageBean(it.message))
        }

        if (body.isNotEmpty()) {
            bodyData(EventUtils.createMessageBean("Information:"))
            body.forEach { bodyData(EventUtils.createMessageBean(it)) }
        }
    }

    private class ReferenceToEvent(val eventId: String) : IBodyData {
        val type: String
            get() = TYPE

        companion object {
            const val TYPE = "reference"
        }
    }
}