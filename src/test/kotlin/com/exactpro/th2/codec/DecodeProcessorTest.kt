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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.test.assertIs

class DecodeProcessorTest {

    @Test
    fun `error message check`() {
        val originalProtocol = "xml"
        val originalProtocols = setOf(originalProtocol)
        val wrongProtocol = "http"

        val processor = DecodeProcessor(TestCodec(true), originalProtocols, CODEC_EVENT_ID.toProto(), false) { }
        val batch = GroupBatch(
            book = BOOK_NAME,
            sessionGroup = SESSION_GROUP_NAME,
            groups = mutableListOf(MessageGroup(listOf(
                RawMessage(MESSAGE_ID, protocol = originalProtocol),
                RawMessage(MESSAGE_ID, protocol = wrongProtocol),
                ParsedMessage(MESSAGE_ID, type = MESSAGE_TYPE, protocol = originalProtocol),
                RawMessage(MESSAGE_ID, protocol = originalProtocol),
                RawMessage()
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(5, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        val message0 = assertIs<ParsedMessage>(result.groups[0].messages[0])

        message0.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(originalProtocol, it.protocol)
        }

        val message1 = assertIs<RawMessage>(result.groups[0].messages[1])

        message1.let {
            Assertions.assertEquals(wrongProtocol, it.protocol)
        }

        val message2 = assertIs<ParsedMessage>(result.groups[0].messages[2])

        message2.let {
            Assertions.assertEquals("test-type", it.type)
            Assertions.assertEquals(originalProtocol, it.protocol)
        }

        val message3 = assertIs<ParsedMessage>(result.groups[0].messages[3])

        message3.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(originalProtocol, it.protocol)
        }

        val message4 = assertIs<ParsedMessage>(result.groups[0].messages[4])

        message4.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(originalProtocol, it.protocol)
        }
    }

    companion object {
        const val MESSAGE_TYPE = "test-type"
    }
}

class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup): MessageGroup = MessageGroup()

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return MessageGroup()
    }
}