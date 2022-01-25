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
        val originalProtocol = "xml"
        val differentProtocol = "http"

        val processor = DecodeProcessor(TestCodec(false), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = differentProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = differentProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `simple test - encode`() {
        val originalProtocol = "xml"
        val differentProtocol = "http"

        val processor = EncodeProcessor(TestCodec(false), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = differentProtocol
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = differentProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on failed protocol check - encode`() {
        val originalProtocol = "xml"
        val wrongProtocol = "http"

        val processor = EncodeProcessor(TestCodec(false), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = wrongProtocol
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    // no protocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on thrown - encode`() {
        val originalProtocol = "xml"
        val wrongProtocol = "http"

        val processor = EncodeProcessor(TestCodec(true), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += Message.newBuilder().apply {
                    metadataBuilder.protocol = wrongProtocol
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = originalProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
            }.build())
        }.build()

        val result = processor.process(batch)

        Assertions.assertEquals(0, result.groupsCount) {"Wrong batch size"}
    }

    @Test
    fun `error message on thrown - decode`() {
        val originalProtocol = "xml"
        val wrongProtocol = "http"

        val processor = DecodeProcessor(TestCodec(true), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = wrongProtocol
                }
                this += Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = wrongProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
            }.build())
        }.build()


        val result = processor.process(batch)

        Assertions.assertEquals(1, result.groupsCount) {"Wrong batch size"}
        Assertions.assertEquals(4, result.getGroups(0).messagesList.size) {"group of outgoing messages must be the same size"}

        Assertions.assertTrue(result.getGroups(0).messagesList[0].hasMessage())
        result.getGroups(0).messagesList[0].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[1].hasRawMessage())
        result.getGroups(0).messagesList[1].rawMessage.let {
            Assertions.assertEquals(wrongProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[2].hasMessage())
        result.getGroups(0).messagesList[2].message.let {
            Assertions.assertEquals( "test-type", it.messageType)
            Assertions.assertEquals(wrongProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[3].hasMessage())
        result.getGroups(0).messagesList[3].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }
    }

    @Test
    fun `error message on failed protocol check - decode`() {
        val originalProtocol = "xml"
        val wrongProtocol = "http"

        val processor = DecodeProcessor(TestCodec(false), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }
                this += RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = wrongProtocol
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
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[1].hasRawMessage())
        result.getGroups(0).messagesList[1].rawMessage.let {
            Assertions.assertEquals(wrongProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[2].hasMessage())
        result.getGroups(0).messagesList[2].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }

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