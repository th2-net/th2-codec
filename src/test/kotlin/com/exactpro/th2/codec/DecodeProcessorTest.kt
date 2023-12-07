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
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.codec.AbstractCodecProcessor.Process.DECODE
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertTrue

class DecodeProcessorTest {

    @ParameterizedTest
    @EnumSource(Protocol::class)
    fun `error message check`(protocol: Protocol) {
        val originalProtocol = "xml"
        val originalProtocols = setOf(originalProtocol)
        val wrongProtocol = "http"

        val processor = UniversalCodec(
            TestCodec(true),
            originalProtocols,
            useParentEventId = false,
            enabledVerticalScaling = false,
            protocol = protocol,
            process = DECODE,
            eventProcessor = EventProcessor(CODEC_EVENT_ID_BOOK_A.toProto()) {},
            config = Configuration()
        )
        val batch = getNewBatchBuilder(protocol, BOOK_NAME_A, SESSION_GROUP_NAME)
            .addNewRawMessage(MESSAGE_ID, protocol = originalProtocol)
            .addNewRawMessage(MESSAGE_ID, protocol = wrongProtocol)
            .addNewParsedMessage(MESSAGE_ID, type = MESSAGE_TYPE, protocol = originalProtocol)
            .addNewRawMessage(MESSAGE_ID, protocol = originalProtocol)
            .addNewRawMessage()
            .build()

        val result = processor.process(batch)

        Assertions.assertEquals(5, result.groupCursor.messagesCount) { "group of outgoing messages must be the same size" }

        result.groupCursor.messageCursor.let {
            assertTrue(it.isParsed)
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.asParsed.type)
            Assertions.assertEquals(originalProtocol, it.protocol)

            result.groupCursor.currentMessageIndex = 1
            assertTrue(it.isRaw)
            Assertions.assertEquals(wrongProtocol, it.protocol)

            result.groupCursor.currentMessageIndex = 2
            assertTrue(it.isParsed)
            Assertions.assertEquals(MESSAGE_TYPE, it.asParsed.type)
            Assertions.assertEquals(originalProtocol, it.protocol)

            result.groupCursor.currentMessageIndex = 3
            assertTrue(it.isParsed)
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.asParsed.type)
            Assertions.assertEquals(originalProtocol, it.protocol)

            result.groupCursor.currentMessageIndex = 4
            assertTrue(it.isParsed)
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.asParsed.type)
            Assertions.assertEquals(originalProtocol, it.protocol)
        }
    }

    companion object {
        const val MESSAGE_TYPE = "test-type"
    }
}

class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = MessageGroup()

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return MessageGroup()
    }

    override fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = ProtoMessageGroup.getDefaultInstance()

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return ProtoMessageGroup.getDefaultInstance()
    }
}