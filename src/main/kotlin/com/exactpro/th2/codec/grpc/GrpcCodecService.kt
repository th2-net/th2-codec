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

package com.exactpro.th2.codec.grpc

import com.exactpro.th2.codec.CodecException
import com.exactpro.th2.codec.EventProcessor
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.utils.message.sessionAlias
import io.grpc.Status
import io.grpc.stub.StreamObserver

class GrpcCodecService(
    grpcRouter: GrpcRouter,
    private val generalDecodeFunc: ((MessageGroupBatch) -> MessageGroupBatch),
    private val generalEncodeFunc: ((MessageGroupBatch) -> MessageGroupBatch),
    private val isFirstCodecInPipeline: Boolean,
    private val eventProcessor: EventProcessor
) : CodecGrpc.CodecImplBase() {

    private val nextCodec = try {
        grpcRouter.getService(AsyncCodecService::class.java)
    } catch (_: Exception) {
        null
    }

    private val MessageGroupBatch.sessionAlias: String
        get() {
            var sessionAlias: String? = null
            for (group in groupsList) {
                for (message in group.messagesList) {
                    when (sessionAlias) {
                        null -> sessionAlias = message.sessionAlias
                        else -> require(sessionAlias == message.sessionAlias) { "Batch contains more than one session alias" }
                    }
                }
            }
            return requireNotNull(sessionAlias) { "Batch is empty" }
        }

    override fun decode(batch: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        try {
            val parsed = generalDecodeFunc(batch)
            if (parsed.anyMessage(AnyMessage::hasRawMessage)) {
                if (nextCodec == null) throw CodecException("grpc codec pipeline output contains raw messages after decoding")
                nextCodec.decode(parsed, mapOf("session_alias" to parsed.sessionAlias), responseObserver)
            } else {
                responseObserver.onNext(parsed)
                responseObserver.onCompleted()
            }
        } catch (e: Exception) {
            val errorMessage = "'decode' rpc call exception: ${e.message}"
            eventProcessor.onErrorEvent(message = errorMessage, messagesIds = batch.messageIds, cause = e)
            responseObserver.onError(Status.INTERNAL.withDescription(errorMessage).withCause(e).asException())
        }
    }

    override fun encode(batch: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val nextCodecObserver = object : StreamObserver<MessageGroupBatch> by responseObserver {
            override fun onNext(value: MessageGroupBatch) {
                try {
                    val encoded = generalEncodeFunc(value)

                    if (isFirstCodecInPipeline && encoded.anyMessage(AnyMessage::hasMessage)) {
                        throw CodecException("grpc codec pipeline output contains parsed messages after encoding")
                    }

                    responseObserver.onNext(encoded)
                    responseObserver.onCompleted()
                } catch (e: Exception) {
                    val errorMessage = "'encode' rpc call exception: ${e.message}"
                    eventProcessor.onErrorEvent(message = errorMessage, messagesIds = batch.messageIds, cause = e)
                    responseObserver.onError(Status.INTERNAL.withDescription(errorMessage).withCause(e).asException())
                }
            }

            override fun onCompleted() {}
        }

        try {
            if (nextCodec != null && batch.anyMessage(AnyMessage::hasMessage)) {
                nextCodec.encode(batch, mapOf("session_alias" to batch.sessionAlias), nextCodecObserver)
            } else {
                nextCodecObserver.onNext(batch) // onCompleted invoked in onNext method
            }
        } catch (e: Exception) {
            val errorMessage = "'encode' rpc call exception: ${e.message}"
            eventProcessor.onErrorEvent(message = errorMessage, messagesIds = batch.messageIds, cause = e)
            responseObserver.onError(Status.INTERNAL.withDescription(errorMessage).withCause(e).asException())
        }
    }

    private fun MessageGroupBatch.anyMessage(predicate: (AnyMessage) -> Boolean) =
        groupsList.any { it.messagesList.any(predicate) }
}

private val MessageGroupBatch.messageIds get() = groupsList.flatMap { it.messageIds }