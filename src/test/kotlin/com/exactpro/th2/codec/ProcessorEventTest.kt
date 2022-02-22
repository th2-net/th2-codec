/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.exactpro.th2.codec

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.UUID

class ProcessorEventTest {

    @Test
    fun `simple test`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(false), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(false), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)

        decodeProcessor.process(testingBatch)
        encodeProcessor.process(testingBatch)

        verify(onEvent, times(0)).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(true), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(true), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)

        decodeProcessor.process(testingBatch) /* root event (1) and 1 for each EventID (4) = 5 */
        verify(onEvent, times(5)).invoke(any(), anyOrNull())

        reset(onEvent)

        encodeProcessor.process(testingBatch) /* root event (1) and 1 for each EventID (4) + 1 size batch event = 6 */
        verify(onEvent, times(6)).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test - with warnings`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)

        decodeProcessor.process(testingBatch) /* root error event (1) + 1 for each EventID (4) + 2 warnings for each EventID (8) + 2 root warnings (2) = 15 */
        verify(onEvent, times(15)).invoke(any(), anyOrNull())

        reset(onEvent)

        encodeProcessor.process(testingBatch)/* root error event (1) + 1 for each EventID (4) + 2 warnings for each EventID (8) + 2 root warnings (2) + 1 size of batch event = 16 */
        verify(onEvent, times(16)).invoke(any(), anyOrNull())
    }

    @Test
    fun `simple test - with warnings`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, onEvent = onEvent)

        decodeProcessor.process(testingBatch) /* 2 warnings for each EventID (8) + 2 root warnings (2) = 10 */
        verify(onEvent, times(10)).invoke(any(), anyOrNull())

        reset(onEvent)

        encodeProcessor.process(testingBatch) /* 2 warnings for each EventID (8) + 2 root warnings (2) = 10 */
        verify(onEvent, times(10)).invoke(any(), anyOrNull())
    }

    @Test
    fun `simple test - general with warnings`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, onEvent = onEvent)

        decodeProcessor.process(testingBatch) /* 2 root warnings = 2 */
        verify(onEvent, times(2)).invoke(any(), anyOrNull())

        reset(onEvent)

        encodeProcessor.process(testingBatch) /* 2 root warnings = 2 */
        verify(onEvent, times(2)).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test - general with warnings`() {
        val onEvent = mock<(Event, String?)->Unit>()

        val decodeProcessor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, onEvent = onEvent)
        val encodeProcessor = EncodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, onEvent = onEvent)

        decodeProcessor.process(testingBatch) /* 1 root error + 2 root warnings = 3 */
        verify(onEvent, times(3)).invoke(any(), anyOrNull())

        reset(onEvent)

        encodeProcessor.process(testingBatch) /* 1 root error + 2 root warnings + 1 size batch event = 4 */
        verify(onEvent, times(4)).invoke(any(), anyOrNull())
    }

    companion object {
        private val testingBatch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    setProtocol(ORIGINAL_PROTOCOL)
                    parentEventId = EventID.newBuilder().setId(UUID.randomUUID().toString()).build()
                }
                this += RawMessage.newBuilder().apply {
                    setProtocol(WRONG_PROTOCOL)
                    parentEventId = EventID.newBuilder().setId(UUID.randomUUID().toString()).build()
                }
                this += RawMessage.newBuilder().apply {
                    setProtocol(WRONG_PROTOCOL)
                    parentEventId = EventID.newBuilder().setId(UUID.randomUUID().toString()).build()
                }
                this += RawMessage.newBuilder().apply {
                    setProtocol(ORIGINAL_PROTOCOL)
                    parentEventId = EventID.newBuilder().setId(UUID.randomUUID().toString()).build()
                }
            }.build())
        }.build()


        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        const val WARN_MESSAGE = "test warn"
    }

    class TestCodec(private val throwEx: Boolean, private val warningsCount: Int = 0) : IPipelineCodec {
        override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
            repeat(warningsCount) {
                context.warning(WARN_MESSAGE)
            }

            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return messageGroup
        }

        override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
            repeat(warningsCount) {
                context.warning(WARN_MESSAGE)
            }

            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return messageGroup
        }
    }
}

