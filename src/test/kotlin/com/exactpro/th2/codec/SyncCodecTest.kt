/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroupBatchMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.TextFormat.shortDebugString
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify

class SyncCodecTest {

    private lateinit var messageRouter: MessageRouter<MessageGroupBatch>
    private lateinit var eventRouter: MessageRouter<EventBatch>
    private lateinit var processor: AbstractCodecProcessor

    @BeforeEach
    fun beforeEach() {
        messageRouter = mock { }
        eventRouter = mock { }
        processor = mock {
            on { process(any()) }.thenAnswer { invocation -> invocation.arguments[0] as MessageGroupBatch }
        }
    }

    @Test
    fun `enable external queue routing test`() {
        TestSyncCodec(messageRouter, eventRouter, processor, CODEC_EVENT_ID, true).apply {
            start(SOURCE_ATTRIBUTE, TARGET_ATTRIBUTE)
        }.use { codec ->
            for (batch in ORIGIN_BATCH_SET) {
                codec.handle(DELIVERY_METADATA, batch)
                verify(messageRouter, times(1).description("Send ${shortDebugString(batch)} batch")).sendAll(
                    eq(batch),
                    any()
                )
                verify(processor, times(1).description("Process ${shortDebugString(batch)} batch")).process(eq(batch))
            }

            val completeBatches = setOf(
                BATCH_WITH_PARSED.toBuilder().setMetadata(BATCH_METADATA).build(),
                BATCH_WITH_RAW.toBuilder().setMetadata(BATCH_METADATA).build()
            )

            for (completeBatch in completeBatches) {
                codec.handle(DELIVERY_METADATA, completeBatch)
                verify(
                    messageRouter,
                    times(1).description("Send ${shortDebugString(completeBatch)} complete batch")
                ).sendExclusive(eq(BATCH_METADATA.externalQueue), eq(completeBatch))
                verify(
                    processor,
                    times(1).description("Process ${shortDebugString(completeBatch)} complete batch")
                ).process(eq(completeBatch))
            }

            val incompleteBatch = BATCH_WITH_BOTH_TYPES.toBuilder().setMetadata(BATCH_METADATA).build()
            codec.handle(DELIVERY_METADATA, incompleteBatch)
            verify(
                messageRouter,
                times(1).description("Send ${shortDebugString(incompleteBatch)} incomplete batch")
            ).sendAll(eq(incompleteBatch), any())
            verify(
                processor,
                times(1).description("Process ${shortDebugString(incompleteBatch)} incomplete batch")
            ).process(eq(incompleteBatch))
        }
    }

    @Test
    fun `disable external queue routing test`() {
        TestSyncCodec(messageRouter, eventRouter, processor, CODEC_EVENT_ID, false).apply {
            start(SOURCE_ATTRIBUTE, TARGET_ATTRIBUTE)
        }.use { codec ->
            val batches = ORIGIN_BATCH_SET + setOf(
                BATCH_WITH_BOTH_TYPES.toBuilder().setMetadata(BATCH_METADATA).build(),
                BATCH_WITH_PARSED.toBuilder().setMetadata(BATCH_METADATA).build(),
                BATCH_WITH_RAW.toBuilder().setMetadata(BATCH_METADATA).build()
            )

            for (batch in batches) {
                codec.handle(DELIVERY_METADATA, batch)
                verify(messageRouter, times(1).description("Send ${shortDebugString(batch)} batch")).sendAll(
                    eq(batch),
                    any()
                )
                verify(processor, times(1).description("Process ${shortDebugString(batch)} batch")).process(eq(batch))
            }
        }
    }

    private class TestSyncCodec(
        messageRouter: MessageRouter<MessageGroupBatch>,
        eventRouter: MessageRouter<EventBatch>,
        processor: AbstractCodecProcessor,
        codecRootID: EventID,
        enabledExternalQueueRouting: Boolean
    ) : AbstractSyncCodec(
        messageRouter,
        eventRouter,
        processor,
        codecRootID,
        enabledExternalQueueRouting
    ) {
        override fun getParentEventId(
            codecRootID: EventID,
            protoSource: MessageGroupBatch,
            protoResult: MessageGroupBatch?
        ): EventID = codecRootID

        override fun checkResult(protoResult: MessageGroupBatch): Boolean = true

        override fun isTransformationComplete(protoResult: MessageGroupBatch): Boolean = protoResult.groupsList
            .flatMap(MessageGroup::getMessagesList)
            .run { all(AnyMessage::hasMessage) || all(AnyMessage::hasRawMessage) }
    }

    companion object {
        private const val SOURCE_ATTRIBUTE = "source"
        private const val TARGET_ATTRIBUTE = "target"

        private val DELIVERY_METADATA = DeliveryMetadata("test-tag")
        private val BATCH_METADATA = MessageGroupBatchMetadata.newBuilder().setExternalQueue("external-queue")

        private val BATCH_WITH_BOTH_TYPES = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.getDefaultInstance()
                this += RawMessage.getDefaultInstance()
            }.build())
        }.build()
        private val BATCH_WITH_RAW = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.getDefaultInstance()
            }.build())
        }.build()
        private val BATCH_WITH_PARSED = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.getDefaultInstance()
            }.build())
        }.build()

        private val ORIGIN_BATCH_SET = setOf(BATCH_WITH_BOTH_TYPES, BATCH_WITH_RAW, BATCH_WITH_PARSED)
    }
}