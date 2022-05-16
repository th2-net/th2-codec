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

import com.exactpro.th2.codec.CodecException
import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.codec.EventProcessor
import com.exactpro.th2.codec.util.checkIfSameSessionAlias
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.sessionAlias
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
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
    }

    private val MessageGroupBatch.sessionAlias get() = (
            groupsList.asSequence()
                .flatMap { it.messagesList }
                .firstOrNull() ?: throw CodecException("Batch does not contain messages")
            )
        .sessionAlias
        .also { alias ->
            if (!checkIfSameSessionAlias(alias)) {
                eventProcessor.onErrorEvent(
                    "Batch contains messages with different session aliases.",
                    messageIds
                )
            }
        }

    override fun decode(batch: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        try {
            val parsed = generalDecodeFunc(batch)
            if (parsed.anyMessage(AnyMessage::hasRawMessage)) {
                if (nextCodec == null) throw DecodeException("codec pipeline output contains raw messages after decoding")
                nextCodec.decode(parsed, mapOf("session_alias" to parsed.sessionAlias), responseObserver)
            } else {
                responseObserver.onNext(parsed)
                responseObserver.onCompleted()
            }
        } catch (t: Throwable) {
            val errorMessage = "'decode' rpc call exception: ${t.message}"
            eventProcessor.onErrorEvent(errorMessage, batch.messageIds, t)
            LOGGER.error(errorMessage, t)
            responseObserver.onError(Status.INTERNAL.withDescription(errorMessage).withCause(t).asException())
        }
    }

    override fun encode(batch: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val nextCodecObserver = object : StreamObserver<MessageGroupBatch> by responseObserver {
            override fun onNext(value: MessageGroupBatch) = responseObserver.onNext(generalEncodeFunc(value))
        }

        try {
            if (nextCodec != null && batch.anyMessage(AnyMessage::hasMessage)) {
                nextCodec.encode(batch, mapOf("session_alias" to batch.sessionAlias), nextCodecObserver)
            } else {
                nextCodecObserver.onNext(batch)
                nextCodecObserver.onCompleted()
            }
        } catch (t: Throwable) {
            val errorMessage = "'encode' rpc call exception: ${t.message}"
            eventProcessor.onErrorEvent(errorMessage, batch.messageIds, t)
            LOGGER.error(errorMessage, t)
            responseObserver.onError(Status.INTERNAL.withDescription(errorMessage).withCause(t).asException())
        }
    }

    private fun MessageGroupBatch.anyMessage(predicate: (AnyMessage) -> Boolean) =
        groupsList.any { it.messagesList.any(predicate) }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}