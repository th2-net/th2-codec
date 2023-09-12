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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.codec.AbstractCodecProcessor.Process.DECODE
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.UUID

class EventTest {

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `simple test - decode`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(false), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(0)).invoke(any())
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `Throw test - decode`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(true), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(5) /* root event (1) and 1 for each EventID (4) = 5 */).invoke(any())
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `Throw test - decode with warnings`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(15) /* root event (1) + 1 for each EventID (4) + 2 warnings for each EventID (8) + 2 root warnings (2) = 15 */).invoke(any())
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `simple test - decode with warnings`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(10) /* 2 warnings for each EventID (8) + 2 root warnings (2) = 10 */).invoke(any())
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `simple test - decode general with warnings`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(false, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), false, process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(2) /* 2 root warnings = 2 */).invoke(any())
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `Throw test - decode general with warnings`(protocol: Protocol) {
        val onEvent = mock<(ProtoEvent) -> Unit>()

        val processor = UniversalCodecProcessor(TestCodec(true, 2), ProcessorTest.ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto(), false, process = DECODE, protocol = protocol, onEvent = onEvent)
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                type = MESSAGE_TYPE,
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        processor.process(batch)

        verify(onEvent, times(3) /* 1 root error + 2 root warnings = 3 */).invoke(any())
    }

    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        const val WARN_MESSAGE = "test warn"
        const val MESSAGE_TYPE = "test_message_type"
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

class LogOnlyEventProcessor : AbstractEventProcessor() {
    override fun storeEvent(
        message: String,
        messagesIds: List<MessageID>,
        body: List<String>
    ): String = DEFAULT_EVENT_ID

    override fun storeErrorEvent(
        message: String,
        messagesIds: List<MessageID>,
        cause: Throwable?,
        additionalBody: List<String>
    ) = DEFAULT_EVENT_ID

    override fun storeEachEvent(warnEvent: String, message: String, events: Set<EventID>) {}
    override fun storeEachErrorEvent(errorEventId: String, message: String, events: Set<EventID>) {}

    companion object {
        private const val DEFAULT_EVENT_ID = "0000"
    }
}