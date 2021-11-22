package com.exactpro.th2.codec

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.messageType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class DecodeProcessorTest {

    @Test
    fun `error message check`() {
        val originalProtocol = "xml"
        val wrongProtocol = "http"

        val processor = DecodeProcessor(TestCodec(true), originalProtocol) { _, _ -> }
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroups(MessageGroup.newBuilder().apply {
                addMessages(AnyMessage.newBuilder().setRawMessage(RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }.build()).build())

                addMessages(AnyMessage.newBuilder().setRawMessage(RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = wrongProtocol
                }.build()).build())

                addMessages(AnyMessage.newBuilder().setMessage(Message.newBuilder().apply {
                    messageType = "test-type"
                    metadataBuilder.protocol = originalProtocol
                }.build()).build())

                addMessages(AnyMessage.newBuilder().setRawMessage(RawMessage.newBuilder().apply {
                    metadataBuilder.protocol = originalProtocol
                }.build()).build())
            }.build())
        }.build()


        val result = processor.process(batch)

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
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }

        Assertions.assertTrue(result.getGroups(0).messagesList[3].hasMessage())
        result.getGroups(0).messagesList[3].message.let {
            Assertions.assertEquals(ERROR_TYPE_MESSAGE, it.messageType)
            Assertions.assertEquals(originalProtocol, it.metadata.protocol)
        }

    }
}

class TestCodec(private val throwEx: Boolean) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup): MessageGroup = MessageGroup.getDefaultInstance()

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        if (throwEx) {
            throw NullPointerException("Simple null pointer exception")
        }
        return MessageGroup.getDefaultInstance()
    }
}