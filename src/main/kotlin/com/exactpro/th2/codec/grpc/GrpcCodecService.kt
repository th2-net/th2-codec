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

import com.exactpro.th2.codec.DecodeException
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class GrpcCodecService(grpcRouter: GrpcRouter) : CodecGrpc.CodecImplBase() {

    private val nextCodec = try {
        grpcRouter.getService(AsyncCodecService::class.java)
    } catch (t: Throwable) {
        null
    }

    var generalDecoderListener: ((MessageGroupBatch) -> MessageGroupBatch)? = null
    var generalEncoderListener: ((MessageGroupBatch) -> MessageGroupBatch)? = null

    override fun decode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val decode = generalDecoderListener ?: return super.decode(message, responseObserver)
        val nextRecode = if (nextCodec == null) null else nextCodec::decode
        recode(message, decode, responseObserver, nextRecode, AnyMessage::hasRawMessage)
    }

    override fun encode(message: MessageGroupBatch, responseObserver: StreamObserver<MessageGroupBatch>) {
        val encode = generalEncoderListener ?: return super.decode(message, responseObserver)
        val nextRecode = if (nextCodec == null) null else nextCodec::decode // TODO: encode
        recode(message, encode, responseObserver, nextRecode,  AnyMessage::hasMessage)
    }

    private fun MessageGroupBatch.anyMessage(predicate: (AnyMessage) -> Boolean) =
        groupsList.asSequence().flatMap { it.messagesList }.any(predicate)

    private fun recode(
        message: MessageGroupBatch,
        recodeFunction: (MessageGroupBatch) -> MessageGroupBatch,
        responseObserver: StreamObserver<MessageGroupBatch>,
        nextRecode: ((MessageGroupBatch, StreamObserver<MessageGroupBatch>) -> Unit)?,
        needToBeRecoded: (AnyMessage) -> Boolean
    ) {
        runCatching {
            recodeFunction(message)
        }
            .onSuccess {
                if (nextRecode != null && it.anyMessage(needToBeRecoded) ) {
                    nextRecode(it, responseObserver)
                }

                responseObserver.onNext(it)
                responseObserver.onCompleted()
            }.onFailure {
                responseObserver.onError(DecodeException(it))
            }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}