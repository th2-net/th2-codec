/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.UUID

class EventTest {
    private val onEvent = mock<(Event, String?)->Unit>()
    private val eventProcessor = EventProcessor(onEvent)

    @Test
    fun `simple test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ProcessorTest.ORIGINAL_PROTOCOLS, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(0)).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test - decode`() {
        val processor = DecodeProcessor(TestCodec(true), ProcessorTest.ORIGINAL_PROTOCOLS, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(5) /* root event (1) and 1 for each EventID (4) = 5 */).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test - decode with warnings`() {
        val processor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(15) /* root event (1) + 1 for each EventID (4) + 2 warnings for each EventID (8) + 2 root warnings (2) = 15 */).invoke(any(), anyOrNull())
    }

    @Test
    fun `simple test - decode with warnings`() {
        val processor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(10) /* 2 warnings for each EventID (8) + 2 root warnings (2) = 10 */).invoke(any(), anyOrNull())
    }

    @Test
    fun `simple test - decode general with warnings`() {
        val processor = DecodeProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(2) /* 2 root warnings = 2 */).invoke(any(), anyOrNull())
    }

    @Test
    fun `Throw test - decode general with warnings`() {
        val processor = DecodeProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, false, eventProcessor = eventProcessor)
        val batch = MessageGroupBatch.newBuilder().apply {
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

        processor.process(batch)

        verify(onEvent, times(3) /* 1 root error + 2 root warnings = 3 */).invoke(any(), anyOrNull())
    }

    companion object {
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