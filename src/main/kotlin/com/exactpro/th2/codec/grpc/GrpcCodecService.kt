/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.grpc

import com.exactpro.th2.codec.EventProcessor
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import io.grpc.Status
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class GrpcCodecService(
    grpcRouter: GrpcRouter,
    private val generalDecodeFunc: ((MessageGroupBatch) -> MessageGroupBatch),
    private val generalEncodeFunc: ((MessageGroupBatch) -> MessageGroupBatch),
    onEvent: (event: Event, parentId: String?) -> Unit
) : CodecGrpc.CodecImplBase() {

    private val eventProcessor = EventProcessor(onEvent)

    private val nextCodec = try {
        grpcRouter.getService(AsyncCodecService::class.java)
    } catch (t: Throwable) {
        null
    }.apply {
        LOGGER.info { "Next codec in pipeline stub: $this" }
    }

    private val MessageGroupBatch.sessionAlias get() = getGroups(0)
        .getMessages(0).sessionAlias
        .also { if (!checkIfSameSessionAlias(it)) eventProcessor.onErrorEvent("Batch contains messages with different session aliases.") }

    private fun MessageGroupBatch.checkIfSameSessionAlias(alias: String) =
        groupsList.all { groups -> groups.messagesList.all { it.sessionAlias == alias } }

    private val AnyMessage.sessionAlias get() = if (hasMessage()) message.sessionAlias else rawMessage.sessionAlias

    override fun decode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        try {
            val parsed = generalDecodeFunc(message)
            if (parsed.anyMessage(AnyMessage::hasRawMessage)) {
                if (nextCodec == null) {
                    eventProcessor.onErrorEvent("codec pipeline output contains raw messages")
                } else {
                    nextCodec.decode(parsed, mapOf("session_alias" to parsed.sessionAlias), responseObserver)
                }
            } else {
                responseObserver.onNext(parsed)
                responseObserver.onCompleted()
            }
        } catch (t: Throwable) {
            eventProcessor.onErrorEvent("'decode' rpc call exception", cause = t)
            LOGGER.error(t) { "'decode' rpc call exception" }
            responseObserver.onError(Status.INTERNAL.withDescription(t.message).withCause(t).asException())
        }
    }

    override fun encode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val nextCodecObserver = object : StreamObserver<MessageGroupBatch> by responseObserver {
            override fun onNext(value: MessageGroupBatch) = responseObserver.onNext(generalEncodeFunc(value))
        }

        try {
            if (nextCodec != null && message.anyMessage(AnyMessage::hasMessage)) {
                nextCodec.encode(message, mapOf("session_alias" to message.sessionAlias), nextCodecObserver)
            } else {
                nextCodecObserver.onNext(message)
                nextCodecObserver.onCompleted()
            }
        } catch (t: Throwable) {
            eventProcessor.onErrorEvent("'encode' rpc call exception", cause = t)
            LOGGER.error(t) { "'encode' rpc call exception" }
            responseObserver.onError(Status.INTERNAL.withDescription(t.message).withCause(t).asException())
        }
    }

    private fun MessageGroupBatch.anyMessage(predicate: (AnyMessage) -> Boolean) =
        groupsList.any { it.messagesList.any(predicate) }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}