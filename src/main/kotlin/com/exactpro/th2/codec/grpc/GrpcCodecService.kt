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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class GrpcCodecService(
    grpcRouter: GrpcRouter,
    private val generalDecodeFunc: ((MessageGroupBatch) -> MessageGroupBatch),
    private val generalEncodeFunc: ((MessageGroupBatch) -> MessageGroupBatch)
) : CodecGrpc.CodecImplBase() {

    private val nextCodec = try {
        grpcRouter.getService(AsyncCodecService::class.java)
    } catch (t: Throwable) {
        null
    }.apply {
        LOGGER.info { "Next codec in pipeline stub: $this" }
    }

    private fun getSessionAlias(message: MessageGroupBatch) = message.groupsList
        .first().messagesList
        .first().rawMessage.metadata.id.connectionId.sessionAlias

    override fun decode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        try {
            val parsed = generalDecodeFunc(message)
            if (nextCodec != null && parsed.anyMessage(AnyMessage::hasRawMessage) ) {
                nextCodec.decode(parsed, mapOf("session-alias" to getSessionAlias(parsed)), responseObserver)
            } else {
                responseObserver.onNext(parsed)
                responseObserver.onCompleted()
            }
        } catch (t: Throwable) {
            LOGGER.error(t) { "'decode' rpc call exception" }
            responseObserver.onError(t)
        }
    }

    override fun encode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val nextCodecObserver = object : StreamObserver<MessageGroupBatch> {
            override fun onNext(value: MessageGroupBatch) {
                val raw = generalEncodeFunc(value)
                responseObserver.onNext(raw)
            }
            override fun onError(t: Throwable) = responseObserver.onError(t)
            override fun onCompleted() = responseObserver.onCompleted()
        }

        try {
            if (nextCodec != null && message.anyMessage(AnyMessage::hasMessage) ) {
                nextCodec.encode(message, mapOf("session-alias" to getSessionAlias(message)), nextCodecObserver)
            } else {
                nextCodecObserver.onNext(message)
                nextCodecObserver.onCompleted()
            }
        } catch (t: Throwable) {
            LOGGER.error(t) { "'encode' rpc call exception" }
            responseObserver.onError(t)
        }
    }

    private fun MessageGroupBatch.anyMessage(predicate: (AnyMessage) -> Boolean) =
        groupsList.asSequence().flatMap { it.messagesList }.any(predicate)

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}