/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.UUID

class EventTest {

    @Test
    fun `simple test - decode`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(false), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(0)).invoke(any())
    }

    @Test
    fun `Throw test - decode`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(true), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(5) /* root event (1) and 1 for each EventID (4) = 5 */).invoke(any())
    }

    @Test
    fun `Throw test - decode with warnings`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(15) /* root event (1) + 1 for each EventID (4) + 2 warnings for each EventID (8) + 2 root warnings (2) = 15 */).invoke(any())
    }

    @Test
    fun `simple test - decode with warnings`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(10) /* 2 warnings for each EventID (8) + 2 root warnings (2) = 10 */).invoke(any())
    }

    @Test
    fun `simple test - decode general with warnings`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), false, onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(2) /* 2 root warnings = 2 */).invoke(any())
    }

    @Test
    fun `Throw test - decode general with warnings`() {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), false, onEvent = onEvent)
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                })
            )
        }

        processor.process(batch)

        verify(onEvent, times(3) /* 1 root error + 2 root warnings = 3 */).invoke(any())
    }

    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        const val WARN_MESSAGE = "test warn"
    }

    class TestCodec(private val throwEx: Boolean, private val warningsCount: Int = 0) : IPipelineCodec {
        override fun encode(messageGroup: DemoMessageGroup, context: IReportingContext): DemoMessageGroup {
            repeat(warningsCount) {
                context.warning(WARN_MESSAGE)
            }

            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return messageGroup
        }

        override fun decode(messageGroup: DemoMessageGroup, context: IReportingContext): DemoMessageGroup {
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

