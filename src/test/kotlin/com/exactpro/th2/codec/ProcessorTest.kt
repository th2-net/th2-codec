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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertIs

class ProcessorTest {

    @Test
    fun `simple test - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                RawMessage(protocol = WRONG_PROTOCOL),
                RawMessage(protocol = WRONG_PROTOCOL),
                RawMessage(protocol = ORIGINAL_PROTOCOL)
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `other protocol in raw message test - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(RawMessage(protocol = WRONG_PROTOCOL))))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }


    @Test
    fun `one parsed message in group test - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(
                MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL))),
                MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL))),
                MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE), ParsedMessage(type = MESSAGE_TYPE)))
            )
        )

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `multiple protocols test - decode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = TransportDecodeProcessor(TestCodec(false), originalProtocols, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(
                MessageGroup(listOf(
                    ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                    RawMessage(protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = ORIGINAL_PROTOCOL)
                )),
                MessageGroup(listOf(
                    ParsedMessage(type = MESSAGE_TYPE, protocol = secondOriginalProtocol),
                    RawMessage(protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = secondOriginalProtocol)
                ))
            )
        )

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `simple test - encode`() {
        val processor = TransportEncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                    ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL, ),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = ORIGINAL_PROTOCOL)
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `other protocol in parsed message test - encode`() {
        val processor = TransportEncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL))))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `one raw message in group test - encode`() {
        val processor = TransportEncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(
                MessageGroup(listOf(RawMessage(protocol = ORIGINAL_PROTOCOL))),
                MessageGroup(listOf(RawMessage(protocol = WRONG_PROTOCOL))),
                MessageGroup(listOf(RawMessage(), RawMessage()))
            )
        )

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groups.size) { "Wrong batch size" }
    }


    @Test
    fun `multiple protocols test - encode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = TransportEncodeProcessor(TestCodec(false), originalProtocols, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(
                MessageGroup(listOf(
                    ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = ORIGINAL_PROTOCOL)
                )),
                MessageGroup(listOf(
                    ParsedMessage(type = MESSAGE_TYPE, protocol = secondOriginalProtocol),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                    ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                    RawMessage(protocol = secondOriginalProtocol)
                ))
            )
        )

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on failed protocol check - encode`() {
        val processor = TransportEncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                ParsedMessage(type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                ParsedMessage(type = MESSAGE_TYPE, protocol = WRONG_PROTOCOL),
                ParsedMessage(type = MESSAGE_TYPE),
                RawMessage(protocol = ORIGINAL_PROTOCOL)
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on thrown - encode`() {
        val processor = TransportEncodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                    ParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = EventTest.ORIGINAL_PROTOCOL),
                    ParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = EventTest.WRONG_PROTOCOL),
                    ParsedMessage(id = MESSAGE_ID, type = MESSAGE_TYPE, protocol = ORIGINAL_PROTOCOL),
                    RawMessage(protocol = ORIGINAL_PROTOCOL)
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on thrown - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                RawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                ),
                RawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                ),
                ParsedMessage(
                    type = MESSAGE_TYPE,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = WRONG_PROTOCOL
                ),
                RawMessage(
                    id = MESSAGE_ID,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = ORIGINAL_PROTOCOL
                )
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(4, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<ParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<RawMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("test-type", it.type)
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
            Assertions.assertFalse(ERROR_EVENT_ID in it.body)
            Assertions.assertFalse(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[3]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }


    @Test
    fun `error message on failed protocol check - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                RawMessage(
                    id = MESSAGE_ID,
                    protocol = ORIGINAL_PROTOCOL,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                ),
                RawMessage(
                    id = MESSAGE_ID,
                    protocol = WRONG_PROTOCOL,
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                ),
                RawMessage(eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()))
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(3, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<ParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<RawMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    @Test
    fun `multiple protocol test - decode`() {
        val processor = TransportDecodeProcessor(TestCodec(true), setOf("xml", "json"), CODEC_EVENT_ID.toProto()) { }
        val batch = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP_NAME,
            listOf(MessageGroup(listOf(
                RawMessage(
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = "xml"
                ),
                RawMessage(
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = "json"
                ),
                RawMessage(
                    eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()),
                    protocol = "http"
                ),
                RawMessage(eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()))
            )))
        )

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(4, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<ParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("xml", it.protocol)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("json", it.protocol)
        }

        assertIs<RawMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("http", it.protocol)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("json", it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<RawMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("http", it.protocol)
        }

        assertIs<ParsedMessage>(result.groups[0].messages[3]).let {
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
        override fun encode(messageGroup: MessageGroup): MessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return MessageGroup(listOf(RawMessage()))
        }

        override fun decode(messageGroup: MessageGroup): MessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return MessageGroup(listOf(ParsedMessage(type = MESSAGE_TYPE)))
        }
    }
}