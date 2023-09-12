/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_EVENT_ID
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.codec.AbstractCodecProcessor.Process.DECODE
import com.exactpro.th2.codec.AbstractCodecProcessor.Process.ENCODE
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.AnyMessage as ProtoAnyMessage
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

class ProcessorTest {

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `simple test - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `other protocol in raw message test - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `one parsed message in group test - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .startNewMessageGroup()
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .startNewMessageGroup()
            .addNewParsedMessage(type = MESSAGE_TYPE)
            .addNewParsedMessage(type = MESSAGE_TYPE)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `multiple protocols test - decode`(protocol: Protocol) {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = UniversalCodecProcessor(TestCodec(false), originalProtocols, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .startNewMessageGroup()
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = secondOriginalProtocol)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = secondOriginalProtocol)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `simple test - encode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL, )
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `other protocol in parsed message test - encode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(false), ORIGINAL_PROTOCOLS, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `one raw message in group test - encode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(false), ORIGINAL_PROTOCOLS, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .startNewMessageGroup()
            .addNewRawMessage(protocol = WRONG_PROTOCOL)
            .startNewMessageGroup()
            .addNewRawMessage()
            .addNewRawMessage()
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `multiple protocols test - encode`(protocol: Protocol) {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = UniversalCodecProcessor (TestCodec(false), originalProtocols, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})

        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .startNewMessageGroup()
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = secondOriginalProtocol)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewRawMessage(protocol = secondOriginalProtocol)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `error message on failed protocol check - encode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(false), ORIGINAL_PROTOCOLS, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL)
            .addNewParsedMessage(type = MESSAGE_TYPE)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `error message on thrown - encode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(true), ORIGINAL_PROTOCOLS, process = ENCODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})

        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = EventTest.ORIGINAL_PROTOCOL)
            .addNewParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = EventTest.WRONG_PROTOCOL)
            .addNewParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL)
            .addNewRawMessage(protocol = ORIGINAL_PROTOCOL)
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) { "Wrong batch size" }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `error message on thrown - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(true), ORIGINAL_PROTOCOLS, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewParsedMessage(
                type = MESSAGE_TYPE,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = WRONG_PROTOCOL
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = ORIGINAL_PROTOCOL
            )
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
        Assertions.assertEquals(4, result.groupCursor.messagesCount) { "group of outgoing messages must be the same size" }

        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            val parsedBody = it.body
            Assertions.assertTrue(ERROR_EVENT_ID in parsedBody)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in parsedBody)
        }

        result.groupCursor.currentMessageIndex = 1
        result.groupCursor.messageCursor.asRaw.let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        result.groupCursor.currentMessageIndex = 2
        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(MESSAGE_TYPE, it.type)
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
            val parsedBody = it.body
            Assertions.assertFalse(ERROR_EVENT_ID in parsedBody)
            Assertions.assertFalse(ERROR_CONTENT_FIELD in parsedBody)
        }

        result.groupCursor.currentMessageIndex = 3
        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `error message on failed protocol check - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor (TestCodec(false), ORIGINAL_PROTOCOLS, process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewRawMessage(
                id = MESSAGE_ID,
                protocol = ORIGINAL_PROTOCOL,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
            )
            .addNewRawMessage(
                id = MESSAGE_ID,
                protocol = WRONG_PROTOCOL,
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
            )
            .addNewRawMessage(eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()))
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
        Assertions.assertEquals(3, result.groupCursor.messagesCount) { "group of outgoing messages must be the same size" }


        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        result.groupCursor.currentMessageIndex = 1
        result.groupCursor.messageCursor.asRaw.let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        result.groupCursor.currentMessageIndex = 2
        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `multiple protocol test - decode`(protocol: Protocol) {
        val processor = UniversalCodecProcessor(TestCodec(true), setOf("xml", "json"), process = DECODE, protocol = protocol, eventProcessor = EventProcessor(CODEC_EVENT_ID.toProto()) {})
        val batch = getNewBatchBuilder(protocol, BOOK_NAME, SESSION_GROUP_NAME)
            .addNewRawMessage(
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = "xml"
            )
            .addNewRawMessage(
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = "json"
            )
            .addNewRawMessage(
                eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                protocol = "http"
            )
            .addNewRawMessage(eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()))
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) { "Wrong batch size" }
        Assertions.assertEquals(
            4,
            result.groupCursor.messagesCount
        ) { "group of outgoing messages must be the same size" }

        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("xml", it.protocol)
        }

        result.groupCursor.currentMessageIndex = 1
        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("json", it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        result.groupCursor.currentMessageIndex = 2
        result.groupCursor.messageCursor.asRaw.let {
            Assertions.assertEquals("http", it.protocol)
        }

        result.groupCursor.currentMessageIndex = 3
        result.groupCursor.messageCursor.asParsed.let {
            Assertions.assertEquals("[xml, json]", it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        val ORIGINAL_PROTOCOLS = setOf(ORIGINAL_PROTOCOL)
        const val MESSAGE_TYPE = "test-type"
    }

    class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
        override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return MessageGroup(listOf(RawMessage()))
        }

        override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE)))
        }

        override fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return ProtoMessageGroup.newBuilder()
                .addAllMessages(listOf(
                    ProtoAnyMessage.newBuilder()
                        .setRawMessage(ProtoRawMessage.getDefaultInstance())
                        .build()))
                .build()
        }

        override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return ProtoMessageGroup.newBuilder()
                .addAllMessages(listOf(
                    ProtoAnyMessage.newBuilder()
                        .setMessage(ProtoMessage.newBuilder().setMetadata(
                            MessageMetadata.newBuilder().setMessageType(MESSAGE_TYPE)
                        ))
                        .build()))
                .build()
        }
    }
}