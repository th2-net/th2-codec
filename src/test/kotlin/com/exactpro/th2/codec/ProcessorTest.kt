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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertIs

class ProcessorTest {

    @Test
    fun `simple test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `other protocol in raw message test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }


    @Test
    fun `one parsed message in group test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage()
                    this += DemoParsedMessage()
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `multiple protocols test - decode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = DecodeProcessor(TestCodec(false), originalProtocols, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = secondOriginalProtocol)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                    this += DemoRawMessage(protocol = secondOriginalProtocol)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `simple test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                    this += DemoParsedMessage().apply {
                        type = "test-type"
                        protocol = WRONG_PROTOCOL
                    }
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `other protocol in parsed message test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `one raw message in group test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage(protocol = WRONG_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage()
                    this += DemoRawMessage()
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `multiple protocols test - encode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = EncodeProcessor(TestCodec(false), originalProtocols, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                    this += DemoParsedMessage().apply {
                        type = "test-type"
                        protocol = WRONG_PROTOCOL
                    }
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                }),
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = secondOriginalProtocol)
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                    this += DemoParsedMessage().apply {
                        type = "test-type"
                        protocol = WRONG_PROTOCOL
                    }
                    this += DemoRawMessage(protocol = secondOriginalProtocol)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on failed protocol check - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage(protocol = ORIGINAL_PROTOCOL)
                    this += DemoParsedMessage(protocol = WRONG_PROTOCOL)
                    this += DemoParsedMessage()
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on thrown - encode`() {
        val processor = EncodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoParsedMessage().apply {

                        id = MESSAGE_ID
                        protocol = EventTest.ORIGINAL_PROTOCOL

                    }
                    this += DemoParsedMessage().apply {

                        id = MESSAGE_ID
                        protocol = EventTest.WRONG_PROTOCOL

                    }
                    this += DemoParsedMessage().apply {
                        type = "test-type"

                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL

                    }
                    this += DemoRawMessage(protocol = ORIGINAL_PROTOCOL)
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groups.size) { "Wrong batch size" }
    }

    @Test
    fun `error message on thrown - decode`() {
        val processor = DecodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoParsedMessage().apply {
                        type = "test-type"
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

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(4, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<DemoParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<DemoRawMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("test-type", it.type)
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
            Assertions.assertFalse(ERROR_EVENT_ID in it.body)
            Assertions.assertFalse(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[3]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    @Test
    fun `error message on failed protocol check - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS, CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = ORIGINAL_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        id = MESSAGE_ID
                        protocol = WRONG_PROTOCOL
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage(
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    )
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(3, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<DemoParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<DemoRawMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.protocol)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }

    @Test
    fun `multiple protocol test - decode`() {
        val processor = DecodeProcessor(TestCodec(true), setOf("xml", "json"), CODEC_EVENT_ID.toProto()) { }
        val batch = DemoGroupBatch().apply {
            groups = mutableListOf(
                DemoMessageGroup(messages = mutableListOf<DemoMessage<*>>().apply {
                    this += DemoRawMessage().apply {
                        protocol = "xml"
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        protocol = "json"
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage().apply {
                        protocol = "http"
                        eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString())
                    }
                    this += DemoRawMessage(eventId = CODEC_EVENT_ID.copy(id = UUID.randomUUID().toString()))
                })
            )
        }

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groups.size) { "Wrong batch size" }
        Assertions.assertEquals(4, result.groups[0].messages.size) { "group of outgoing messages must be the same size" }

        assertIs<DemoParsedMessage>(result.groups[0].messages[0]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("xml", it.protocol)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("json", it.protocol)
        }

        assertIs<DemoRawMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("http", it.protocol)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[1]).let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.type)
            Assertions.assertEquals("json", it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }

        assertIs<DemoRawMessage>(result.groups[0].messages[2]).let {
            Assertions.assertEquals("http", it.protocol)
        }

        assertIs<DemoParsedMessage>(result.groups[0].messages[3]).let {
            Assertions.assertEquals("[xml, json]", it.protocol)
            Assertions.assertTrue(ERROR_EVENT_ID in it.body)
            Assertions.assertTrue(ERROR_CONTENT_FIELD in it.body)
        }
    }
    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        val ORIGINAL_PROTOCOLS = setOf(ORIGINAL_PROTOCOL)
    }

    class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
        override fun encode(messageGroup: DemoMessageGroup): DemoMessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return DemoMessageGroup(messages = mutableListOf(DemoRawMessage()))
        }

        override fun decode(messageGroup: DemoMessageGroup): DemoMessageGroup {
            if (throwEx) {
                throw NullPointerException("Simple null pointer exception")
            }
            return DemoMessageGroup(messages = mutableListOf(DemoParsedMessage()))
        }
    }
}

