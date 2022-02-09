/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.plusAssign
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class ProcessorTest {

    @Test
    fun `simple test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }
    @Test
    fun `other protocol in raw message test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }


    @Test
    fun `one parsed message in group test - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.getDefaultInstance()
                this += Message.getDefaultInstance()
            }.build())

        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `multiple protocols test - decode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = DecodeProcessor(TestCodec(false), originalProtocols) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = secondOriginalProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = secondOriginalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `simple test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `other protocol in parsed message test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `one raw message in group test - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.getDefaultInstance()
                this += RawMessage.getDefaultInstance()
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(3, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `multiple protocols test - encode`() {
        val secondOriginalProtocol = "json"
        val originalProtocols = setOf(ORIGINAL_PROTOCOL, secondOriginalProtocol)

        val processor = EncodeProcessor(TestCodec(false), originalProtocols) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = secondOriginalProtocol
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = secondOriginalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(2, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on failed protocol check - encode`() {
        val processor = EncodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder()
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on thrown - encode`() {
        val processor = EncodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on thrown - decode`() {
        val processor = DecodeProcessor(TestCodec(true), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
            }.build())
        }.build()


        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
        Assertions.assertEquals(4, result.getGroups(0).messagesList.size) {"group of outgoing messages must be the same size"}

        Assertions.assertTrue(result.getGroups(0).messagesList[0].hasMessage())
        result.getGroups(0).messagesList[0].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[1].hasRawMessage())
        result.getGroups(0).messagesList[1].rawMessage.let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[2].hasMessage())
        result.getGroups(0).messagesList[2].message.let {
            Assertions.assertEquals( "test-type", it.messageType)
            Assertions.assertEquals(WRONG_PROTOCOL, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[3].hasMessage())
        result.getGroups(0).messagesList[3].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.metadata.protocol)
        }
    }

    @Test
    fun `error message on failed protocol check - decode`() {
        val processor = DecodeProcessor(TestCodec(false), ORIGINAL_PROTOCOLS) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = ORIGINAL_PROTOCOL
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = WRONG_PROTOCOL
                }
                this += RawMessage.getDefaultInstance()
            }.build())
        }.build()


        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
        Assertions.assertEquals(3, result.getGroups(0).messagesList.size) {"group of outgoing messages must be the same size"}

        Assertions.assertTrue(result.getGroups(0).messagesList[0].hasMessage())
        result.getGroups(0).messagesList[0].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[1].hasRawMessage())
        result.getGroups(0).messagesList[1].rawMessage.let {
            Assertions.assertEquals(WRONG_PROTOCOL, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[2].hasMessage())
        result.getGroups(0).messagesList[2].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(ORIGINAL_PROTOCOL, it.metadata.protocol)
        }

    }

    @Test
    fun `multiple protocol test - decode`() {
        val processor = DecodeProcessor(TestCodec(true), setOf("xml", "json")) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = "xml"
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = "json"
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = "http"
                }
                this += RawMessage.getDefaultInstance()
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
        Assertions.assertEquals(4, result.getGroups(0).messagesList.size) {"group of outgoing messages must be the same size"}

        Assertions.assertTrue(result.getGroups(0).messagesList[0].hasMessage())
        result.getGroups(0).messagesList[0].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals("xml", it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[1].hasMessage())
        result.getGroups(0).messagesList[1].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals("json", it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[2].hasRawMessage())
        result.getGroups(0).messagesList[2].rawMessage.let {
            Assertions.assertEquals("http", it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[3].hasMessage())
        result.getGroups(0).messagesList[3].message.let {
            Assertions.assertEquals("[xml, json]", it.metadata.protocol)
        }

    }

    companion object {
        const val ORIGINAL_PROTOCOL = "xml"
        const val WRONG_PROTOCOL = "http"
        val ORIGINAL_PROTOCOLS = setOf(ORIGINAL_PROTOCOL)
    }
}

class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup): MessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return MessageGroup.newBuilder().addMessages(AnyMessage.newBuilder().setRawMessage(RawMessage.getDefaultInstance()).build()).build()
    }

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return MessageGroup.newBuilder().addMessages(AnyMessage.newBuilder().setMessage(Message.getDefaultInstance()).build()).build()
    }
}