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

import com.exactpro.th2.codec.AbstractCodecProcessor.Process.DECODE
import com.exactpro.th2.codec.EventProcessor.Companion.cradleString
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.utils.message.transport.toProto
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.contains
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.withElementAt
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup

class EventTest {
    private val config = Configuration()

    interface Test {
        fun `simple test`(protocol: Protocol)
        fun `throw test`(protocol: Protocol)
        fun `throw test - batch with book A vs codec with book A`(protocol: Protocol)
        fun `throw test - batch with book B vs codec with book A`(protocol: Protocol)
        fun `throw test - with warnings when useParentEventId = true`(protocol: Protocol)
        fun `simple test - with warnings when useParentEventId = true`(protocol: Protocol)
        fun `simple test - with warnings when useParentEventId = false`(protocol: Protocol)
        fun `throw test - with warnings when useParentEventId = false`(protocol: Protocol)
    }

    @Nested
    inner class DecodeTest: Test {
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()
            val codec = UniversalCodec(TestCodec(false),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                ) {},
                config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, never()).invoke(any())
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(
                    BOOK_NAME_A, COMPONENT_NAME, onEvent
                ),
                config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(3)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                            get { scope }.isEqualTo(codec.codecEventId.scope)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                            get { scope }.isEqualTo(codec.codecEventId.scope)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                        }
                    }
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - batch with book A vs codec with book A`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent),
                config = config
            )

            val msgId1 = MESSAGE_ID.copy(sequence = 1)
            val msgId2 = MESSAGE_ID.copy(sequence = 2)
            val msgId3 = MESSAGE_ID.copy(sequence = 3)

            val eventId2A = EventId("$EVENT_ID-2", BOOK_NAME_A, "$EVENT_SCOPE-2", Instant.now())
            val eventId3B = EventId("$EVENT_ID-3", BOOK_NAME_B, "$EVENT_SCOPE-3", Instant.now())

            val batchA = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .startNewMessageGroup()
                .addNewRawMessage(id = msgId1)
                .addNewRawMessage(id = msgId2, eventId = eventId2A)

                .startNewMessageGroup()
                .addNewRawMessage(id = msgId3, eventId = eventId3B)
                .build()

            val batch = codec.decode(batchA)
            assertEquals(2, batch.groupsCount, "batch: $batch")

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(6)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                withElementAt(1) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchA.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchA.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2A.book)
                        get { scope }.isEqualTo(eventId2A.scope)
                    }
                    get { parentId }.isEqualTo(eventId2A.toProto())
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchA.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchA.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[1].id.cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"),"")
                        )
                }
                withElementAt(3) {
                    isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
                }
                withElementAt(4) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[3].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[3].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[3].id)
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchA.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(5) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId3B.book)
                        get { scope }.isEqualTo(eventId3B.scope)
                    }
                    get { parentId }.isEqualTo(eventId3B.toProto())
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[4].id.cradleString}","type":"reference"},
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchA.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - batch with book B vs codec with book A`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent),
                config = config
            )

            val msgId1 = MESSAGE_ID.copy(sequence = 1)
            val msgId2 = MESSAGE_ID.copy(sequence = 2)
            val msgId3 = MESSAGE_ID.copy(sequence = 3)

            val eventId2B = EventId("$EVENT_ID-2", BOOK_NAME_B, "$EVENT_SCOPE-2", Instant.now())
            val eventId3A = EventId("$EVENT_ID-3", BOOK_NAME_A, "$EVENT_SCOPE-3", Instant.now())

            val batchB = getNewBatchBuilder(protocol, BOOK_NAME_B, SESSION_GROUP_NAME)
                .startNewMessageGroup()
                .addNewRawMessage(id = msgId1)
                .addNewRawMessage(id = msgId2, eventId = eventId2B)

                .startNewMessageGroup()
                .addNewRawMessage(id = msgId3, eventId = eventId3A)
                .build()

            val batch = codec.decode(batchB)
            assertEquals(2, batch.groupsCount, "batch: $batch")

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(6)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                withElementAt(1) {
                    isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
                }
                (2 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        val id = captor.allValues[index].id
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(id.bookName)
                            get { scope }.isEqualTo(id.scope)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(id.bookName)
                        }

                        get { type }.isEqualTo("Error")
                        get { status }.isEqualTo(EventStatus.FAILED)
                    }
                }
                withElementAt(2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[1].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[1].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[1].id)
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.apply {
                        hasSize(2)
                        contains(msgId1.toProto(batchB.book!!, SESSION_GROUP_NAME))
                        contains(msgId2.toProto(batchB.book!!, SESSION_GROUP_NAME))
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(3) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2B.book)
                        get { scope }.isEqualTo(eventId2B.scope)
                    }
                    get { parentId }.isEqualTo(eventId2B.toProto())
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchB.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchB.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[2].id.cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(4) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchB.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(5) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId3A.book)
                        get { scope }.isEqualTo(eventId3A.scope)
                    }
                    get { parentId }.isEqualTo(eventId3A.toProto())
                    get { name }.isEqualTo("Failed to decode message group")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[4].id.cradleString}","type":"reference"},
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchB.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - with warnings when useParentEventId = true`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = true, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, times(7)).invoke(any())
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test - with warnings when useParentEventId = true`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = true, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, times(5)).invoke(any())
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test - with warnings when useParentEventId = false`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = false, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, times(3)).invoke(any())
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - with warnings when useParentEventId = false`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = false, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.decode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, times(4)).invoke(any())
        }
    }

    @Nested
    inner class EncodeTest: Test {
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(TestCodec(false),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME
                ) {},
                config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.encode(batch)
            assertEquals(1, result.groupsCount)

            verify(onEvent, never()).invoke(any())
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(
                    BOOK_NAME_A, COMPONENT_NAME, onEvent
                ),
                config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            assertThrows<CodecException> {
                codec.encode(batch)
            }.also { assertEquals("Result batch is empty", it.message) }

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(5)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                        }
                    }
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - batch with book A vs codec with book A`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent),
                config = config
            )

            val msgId1 = MESSAGE_ID.copy(sequence = 1)
            val msgId2 = MESSAGE_ID.copy(sequence = 2)
            val msgId3 = MESSAGE_ID.copy(sequence = 3)

            val eventId2A = EventId("$EVENT_ID-2", BOOK_NAME_A, "$EVENT_SCOPE-2", Instant.now())
            val eventId3B = EventId("$EVENT_ID-3", BOOK_NAME_B, "$EVENT_SCOPE-3", Instant.now())

            val batchA = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .startNewMessageGroup()
                .addNewParsedMessage(id = msgId1, type = MESSAGE_TYPE)
                .addNewParsedMessage(id = msgId2, type = MESSAGE_TYPE, eventId = eventId2A)

                .startNewMessageGroup()
                .addNewParsedMessage(id = msgId3, type = MESSAGE_TYPE, eventId = eventId3B)
                .build()

            assertThrows<CodecException> {
                codec.encode(batchA)
            }.also { assertEquals("Result batch is empty", it.message) }

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(8)).invoke(captor.capture())
            println(captor.allValues)
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                withElementAt(1) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchA.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchA.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchA.getMessage(0, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"},
                            {"data":"${batchA.getMessage(0, 1).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2A.book)
                        get { scope }.isEqualTo(eventId2A.scope)
                    }
                    get { parentId }.isEqualTo(eventId2A.toProto())
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchA.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchA.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[1].id.cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchA.getMessage(0, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"},
                            {"data":"${batchA.getMessage(0, 1).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(3) {
                    isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
                }
                withElementAt(4) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[3].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[3].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[3].id)
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchA.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchA.getMessage(1, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(5) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId3B.book)
                        get { scope }.isEqualTo(eventId3B.scope)
                    }
                    get { parentId }.isEqualTo(eventId3B.toProto())
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[4].id.cradleString}","type":"reference"},
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchA.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchA.getMessage(1, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(6) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Group count in the processed batch (0) is different from the input one (2)")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo("[]")
                }
                withElementAt(7) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2A.book)
                        get { scope }.isEqualTo(eventId2A.scope)
                    }
                    get { parentId }.isEqualTo(eventId2A.toProto())
                    get { name }.isEqualTo("Codec error")
                    get { type }.isEqualTo("CodecError")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }
                        .isEqualTo("[{\"data\":\"Result batch is empty.\",\"type\":\"message\"}]")
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - batch with book B vs codec with book A`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true),
                ProcessorTest.ORIGINAL_PROTOCOLS,
                process = DECODE,
                protocol = protocol,
                eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent),
                config = config
            )

            val msgId1 = MESSAGE_ID.copy(sequence = 1)
            val msgId2 = MESSAGE_ID.copy(sequence = 2)
            val msgId3 = MESSAGE_ID.copy(sequence = 3)

            val eventId2B = EventId("$EVENT_ID-2", BOOK_NAME_B, "$EVENT_SCOPE-2", Instant.now())
            val eventId3A = EventId("$EVENT_ID-3", BOOK_NAME_A, "$EVENT_SCOPE-3", Instant.now())

            val batchB: IGroupBatch = getNewBatchBuilder(protocol, BOOK_NAME_B, SESSION_GROUP_NAME)
                .startNewMessageGroup()
                .addNewParsedMessage(id = msgId1, type = MESSAGE_TYPE)
                .addNewParsedMessage(id = msgId2, type = MESSAGE_TYPE, eventId = eventId2B)

                .startNewMessageGroup()
                .addNewParsedMessage(id = msgId3, type = MESSAGE_TYPE, eventId = eventId3A)
                .build()

            assertThrows<CodecException> {
                codec.encode(batchB)
            }.also { assertEquals("Result batch is empty", it.message) }

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(8)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                withElementAt(1) {
                    isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
                }
                (2 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        val id = captor.allValues[index].id
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(id.bookName)
                            get { scope }.isEqualTo(id.scope)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(id.bookName)
                        }

                        get { status }.isEqualTo(EventStatus.FAILED)
                    }
                }
                withElementAt(2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[1].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[1].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[1].id)
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchB.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchB.book!!, SESSION_GROUP_NAME)
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchB.getMessage(0, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"},
                            {"data":"${batchB.getMessage(0, 1).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(3) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2B.book)
                        get { scope }.isEqualTo(eventId2B.scope)
                    }
                    get { parentId }.isEqualTo(eventId2B.toProto())
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.hasSize(2)
                        .contains(
                            msgId1.toProto(batchB.book!!, SESSION_GROUP_NAME),
                            msgId2.toProto(batchB.book!!, SESSION_GROUP_NAME),
                        )
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[2].id.cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchB.getMessage(0, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"},
                            {"data":"${batchB.getMessage(0, 1).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(4) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchB.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchB.getMessage(1, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(5) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId3A.book)
                        get { scope }.isEqualTo(eventId3A.scope)
                    }
                    get { parentId }.isEqualTo(eventId3A.toProto())
                    get { name }.isEqualTo("Failed to encode message group")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                            [
                            {"data":"This event contains reference to the codec event","type":"message"},
                            {"eventId":"${captor.allValues[4].id.cradleString}","type":"reference"},
                            {"data":"This event contains reference to messages from another book","type":"message"},
                            {"messageId":"${msgId3.toProto(batchB.book!!, SESSION_GROUP_NAME).cradleString}","type":"reference"},
                            {"data":"java.lang.NullPointerException: Simple null pointer exception","type":"message"},
                            {"data":"Information:","type":"message"},
                            {"data":"${batchB.getMessage(1, 0).toReadableBody().replace("\"", "\\\"")}","type":"message"}
                            ]
                        """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(6) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("Group count in the processed batch (0) is different from the input one (2)")
                    get { type }.isEqualTo("Error")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo("[]")
                }
                withElementAt(7) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventId2B.book)
                        get { scope }.isEqualTo(eventId2B.scope)
                    }
                    get { parentId }.isEqualTo(eventId2B.toProto())
                    get { name }.isEqualTo("Codec error")
                    get { type }.isEqualTo("CodecError")
                    get { attachedMessageIdsList }.isEmpty()
                    get { String(body.toByteArray()) }.isEqualTo(
                        "[{\"data\":\"Result batch is empty.\",\"type\":\"message\"}]"
                    )
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - with warnings when useParentEventId = true`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = true, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                )
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            assertThrows<CodecException> {
                codec.encode(batch)
            }.also { assertEquals("Result batch is empty", it.message) }

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(9)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                        }
                    }
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test - with warnings when useParentEventId = true`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = true, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.encode(batch)
            assertEquals(1, result.groupsCount)

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(5)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                        }
                    }
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `simple test - with warnings when useParentEventId = false`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = false, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            val result = codec.encode(batch)
            assertEquals(1, result.groupsCount)

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(3)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                            get { scope }.isEqualTo(codec.codecEventId.scope)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                            get { scope }.isEqualTo(codec.codecEventId.scope)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(codec.codecEventId.bookName)
                        }
                    }
                }
            }
        }
        @ParameterizedTest
        @EnumSource(Protocol::class)
        override fun `throw test - with warnings when useParentEventId = false`(protocol: Protocol) {
            val onEvent = mock<(ProtoEvent) -> Unit>()

            val codec = UniversalCodec(
                TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS,
                useParentEventId = false, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(
                    BOOK_NAME_A,
                    COMPONENT_NAME,
                    onEvent
                ), config = config
            )
            val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
                .addNewRawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = WRONG_PROTOCOL
                )
                .addNewParsedMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID_BOOK_A.copy(id = UUID.randomUUID().toString()),
                    type = MESSAGE_TYPE,
                    protocol = ORIGINAL_PROTOCOL
                )
                .build()

            assertThrows<CodecException> {
                codec.encode(batch)
            }.also { assertEquals("Result batch is empty", it.message) }

            val captor = argumentCaptor<ProtoEvent> { }
            verify(onEvent, times(6)).invoke(captor.capture())
            expectThat(captor.allValues) {
                withElementAt(0) {
                    isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
                }
                (1 until captor.allValues.size).forEach { index ->
                    withElementAt(index) {
                        get { id }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { parentId }.apply {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                            get { scope }.isEqualTo(EVENT_SCOPE)
                        }
                        get { attachedMessageIdsList }.all {
                            get { bookName }.isEqualTo(BOOK_NAME_A)
                        }
                    }
                }
            }
        }
    }

    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        const val WARN_MESSAGE = "test warn"
        const val MESSAGE_TYPE = "test_message_type"

        private fun Any.toReadableBody(): String = when(this) {
            is Message<*> -> this.toJson()
            is AnyMessage -> this.toJson()
            else -> error("Unsupported type: ${this.javaClass}")
        }
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

        override fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
            repeat(warningsCount) {
                context.warning(WARN_MESSAGE)
            }

            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return messageGroup
        }

        override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
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