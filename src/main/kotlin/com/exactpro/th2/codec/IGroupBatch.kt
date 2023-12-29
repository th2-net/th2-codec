/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.grpc.MessageGroupBatch as ProtoMessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.AnyMessage as ProtoAnyMessage
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.exactpro.th2.common.utils.toInstant
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.book
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.subsequence
import com.exactpro.th2.common.utils.message.parentEventId
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString
import io.netty.buffer.Unpooled
import java.lang.ClassCastException
import java.time.Instant

interface IGroupBatch {
    val book: String?
    val sessionGroup: String?
    val groupsCount: Int
    var currentGroupIndex: Int
    val groupCursor: IGroupCursor
    fun getMessage(groupIndex: Int, messageIndex: Int): Any
}

interface IGroupCursor {
    val messagesCount: Int
    var currentMessageIndex: Int
    val messageCursor: IMessageCursor
}

interface IMessageCursor {
    val sessionAlias: String?
    val direction: Direction
    val sequence: Long
    val timestamp: Instant
    val subsequence: List<Int>

    val eventId: String?
    val eventBook: String?
    val eventScope: String?
    val eventTimestamp: Instant?

    val metadata: Map<String, String>
    val protocol: String

    val isRaw: Boolean
    val isParsed: Boolean

    val asRaw: IRawMessage
    val asParsed: IParsedMessage
}

interface IRawMessage : IMessageCursor {
    val rawBody: ByteArray
}

interface IParsedMessage : IMessageCursor {
    val type: String
    val body: Map<String, Any?>
}

class TransportBatchWrapper(val batch: GroupBatch) : IGroupBatch {
    override val book: String get() = batch.book
    override val sessionGroup: String get() = batch.sessionGroup
    override val groupsCount: Int = batch.groups.size
    override var currentGroupIndex: Int = 0
        set(value) {
            if (value !in batch.groups.indices) throw IndexOutOfBoundsException()
            groupCursor.currentMessageIndex = 0 // TODO: empty group
            field = value
        }
    override val groupCursor: IGroupCursor = object : IGroupCursor {
        override val messagesCount: Int get() = batch.groups[currentGroupIndex].messages.size
        override var currentMessageIndex: Int = 0
            set(value) {
                if (value !in batch.groups[currentGroupIndex].messages.indices) throw IndexOutOfBoundsException()
                field = value
            }
        override val messageCursor: IMessageCursor = object : IParsedMessage, IRawMessage {
            private val currentMessage get() = batch.groups[currentGroupIndex].messages[currentMessageIndex]

            override val sessionAlias: String get() = currentMessage.id.sessionAlias
            override val direction: Direction get() = currentMessage.id.direction
            override val sequence: Long get() = currentMessage.id.sequence
            override val timestamp: Instant get() = currentMessage.id.timestamp
            override val subsequence: List<Int> get() = currentMessage.id.subsequence

            override val eventBook: String? get() = currentMessage.eventId?.book
            override val eventScope: String? get() = currentMessage.eventId?.scope
            override val eventTimestamp: Instant? get() = currentMessage.eventId?.timestamp
            override val eventId: String? get() = currentMessage.eventId?.id

            override val metadata: Map<String, String> get() = currentMessage.metadata
            override val protocol: String get() = currentMessage.protocol
            override val isRaw: Boolean get() = currentMessage is RawMessage
            override val isParsed: Boolean get() = currentMessage is ParsedMessage

            override val rawBody: ByteArray get() = (currentMessage as RawMessage).body.toByteArray()
            override val type: String get() = (currentMessage as ParsedMessage).type
            override val body: Map<String, Any?> get() = (currentMessage as ParsedMessage).body

            override val asRaw: IRawMessage get() = if (isRaw) this else throw ClassCastException()
            override val asParsed: IParsedMessage get() = if (isParsed) this else throw ClassCastException()
        }
    }
    override fun getMessage(groupIndex: Int, messageIndex: Int): Message<*> = batch.groups[groupIndex].messages[messageIndex]
    override fun toString(): String = batch.toString()
}

class ProtoBatchWrapper(val batch: ProtoMessageGroupBatch) : IGroupBatch {
    override val book: String? get() = batch.groupsList[0].messagesList[0].book
    override val sessionGroup: String? get() = batch.groupsList[0].messagesList[0].sessionGroup
    override val groupsCount: Int = batch.groupsCount
    override var currentGroupIndex: Int = 0 // TODO: empty group
        set(value) {
            if (value !in 0 until batch.groupsCount) throw IndexOutOfBoundsException()
            groupCursor.currentMessageIndex = 0 // TODO: empty group
            field = value
        }
    override val groupCursor: IGroupCursor = object : IGroupCursor {
        override val messagesCount: Int get() = batch.groupsList[currentGroupIndex].messagesCount
        override var currentMessageIndex: Int = 0
            set(value) {
                if (value !in batch.groupsList[currentGroupIndex].messagesList.indices) throw IndexOutOfBoundsException()
                field = value
            }
        override val messageCursor: IMessageCursor = object : IParsedMessage, IRawMessage {
            private val currentMessage get() = batch.groupsList[currentGroupIndex].messagesList[currentMessageIndex]

            override val sessionAlias: String? get() = currentMessage.sessionAlias
            override val direction: Direction get() = currentMessage.direction.transport
            override val sequence: Long get() = currentMessage.id.sequence
            override val timestamp: Instant get() = currentMessage.id.timestamp.toInstant()
            override val subsequence: List<Int> get() = currentMessage.id.subsequence

            override val eventBook: String? get() = currentMessage.parentEventId?.bookName
            override val eventScope: String? get() = currentMessage.parentEventId?.scope
            override val eventTimestamp: Instant? get() = currentMessage.parentEventId?.startTimestamp?.toInstant()
            override val eventId: String? get() = currentMessage.parentEventId?.id

            override val metadata: Map<String, String> get() = if (currentMessage.hasMessage()) currentMessage.message.metadata.propertiesMap else currentMessage.rawMessage.metadata.propertiesMap
            override val protocol: String get() = if (currentMessage.hasMessage()) currentMessage.message.metadata.protocol else currentMessage.rawMessage.metadata.protocol
            override val isRaw: Boolean get() = currentMessage.hasRawMessage()
            override val isParsed: Boolean get() = currentMessage.hasMessage()

            override val rawBody: ByteArray get() = if (isRaw) currentMessage.rawMessage.body.toByteArray() else error("'rawBody' property only available for raw messages")
            override val type: String get() =  if (isParsed) currentMessage.message.messageType else error("'type' property only available for parsed messages")
            override val body: Map<String, Any?> get() = if (isParsed) currentMessage.message.fieldsMap.mapValues { it.value.simpleValue } else error("'body' property only available for parsed messages")

            override val asRaw: IRawMessage get() = if (isRaw) this else throw ClassCastException()
            override val asParsed: IParsedMessage get() = if (isParsed) this else throw ClassCastException()
        }
    }
    override fun getMessage(groupIndex: Int, messageIndex: Int): AnyMessage =
        batch.getGroups(groupIndex).getMessages(messageIndex)
    override fun toString(): String = batch.toJson()
}

interface IBatchBuilder {
    fun startNewMessageGroup(): IBatchBuilder
    fun addNewRawMessage(
        id: MessageId = MessageId.DEFAULT,
        eventId: EventId? = null,
        metadata: Map<String, String> = emptyMap(),
        protocol: String = "",
        body: ByteArray = byteArrayOf(),
    ): IBatchBuilder
    fun addNewParsedMessage(
        id: MessageId = MessageId.DEFAULT,
        eventId: EventId? = null,
        type: String,
        metadata: Map<String, String> = emptyMap(),
        protocol: String = "",
        body: Map<String, Any?> = emptyMap()
    ): IBatchBuilder
    fun build(): IGroupBatch
}

class TransportBatchBuilder(
    private val book: String,
    private val sessionGroup: String
) : IBatchBuilder {
    private val groups: MutableList<MutableList<Message<*>>> = mutableListOf()

    override fun startNewMessageGroup(): IBatchBuilder {
        groups += mutableListOf<Message<*>>()
        return this
    }

    override fun addNewRawMessage(
        id: MessageId,
        eventId: EventId?,
        metadata: Map<String, String>,
        protocol: String,
        body: ByteArray,
    ): IBatchBuilder {
        if (groups.isEmpty()) startNewMessageGroup()
        groups.last() += RawMessage(id, eventId, metadata, protocol, Unpooled.wrappedBuffer(body))
        return this
    }

    override fun addNewParsedMessage(
        id: MessageId,
        eventId: EventId?,
        type: String,
        metadata: Map<String, String>,
        protocol: String,
        body: Map<String, Any?>
    ): IBatchBuilder {
        if (groups.isEmpty()) startNewMessageGroup()
        groups.last() += ParsedMessage(id, eventId, type, metadata, protocol, body)
        return this
    }

    override fun build(): IGroupBatch {
        return TransportBatchWrapper(GroupBatch(
            book,
            sessionGroup,
            groups.map { MessageGroup(it) }
        ))
    }
}

class ProtoBatchBuilder(
    private val book: String,
    private val sessionGroup: String
) : IBatchBuilder {
    private val groups: MutableList<MutableList<ProtoAnyMessage>> = mutableListOf()

    override fun startNewMessageGroup(): IBatchBuilder {
        groups += mutableListOf<ProtoAnyMessage>()
        return this
    }

    override fun addNewRawMessage(
        id: MessageId,
        eventId: EventId?,
        metadata: Map<String, String>,
        protocol: String,
        body: ByteArray,
    ): IBatchBuilder {
        if (groups.isEmpty()) startNewMessageGroup()
        groups.last() += ProtoAnyMessage.newBuilder().setRawMessage(
            ProtoRawMessage.newBuilder()
                .setBody(ByteString.copyFrom(body))
                .setMetadata(
                    RawMessageMetadata.newBuilder()
                        .putAllProperties(metadata)
                        .setId(
                            MessageID.newBuilder()
                                .setBookName(book)
                                .setDirection(id.direction.proto)
                                .setTimestamp(id.timestamp.toTimestamp())
                                .setSequence(id.sequence)
                                .addAllSubsequence(id.subsequence)
                                .setConnectionId(ConnectionID.newBuilder()
                                    .setSessionAlias(id.sessionAlias)
                                    .setSessionGroup(sessionGroup)
                                    .build())
                        )
                        .setProtocol(protocol)
                )
                .apply { eventId?.let { parentEventId = eventId.toProto() } }
        ).build()
        return this
    }

    private fun createMessageId(id: MessageId): MessageID {
        return MessageID.newBuilder()
            .setBookName(book)
            .setDirection(id.direction.proto)
            .setTimestamp(id.timestamp.toTimestamp())
            .setSequence(id.sequence)
            .addAllSubsequence(id.subsequence)
            .setConnectionId(ConnectionID.newBuilder()
                .setSessionAlias(id.sessionAlias)
                .setSessionGroup(sessionGroup)
                .build())
            .build()
    }

    override fun addNewParsedMessage(
        id: MessageId,
        eventId: EventId?,
        type: String,
        metadata: Map<String, String>,
        protocol: String,
        body: Map<String, Any?>
    ): IBatchBuilder {
        if (groups.isEmpty()) startNewMessageGroup()
        groups.last() += ProtoAnyMessage.newBuilder().setMessage(
            ProtoMessage.newBuilder()
                .setMetadata(MessageMetadata.newBuilder()
                    .putAllProperties(metadata)
                    .setId(createMessageId(id))
                    .setMessageType(type)
                    .setProtocol(protocol)
                )
                .putAllFields(body.mapValues { it.value.toValue() })
                .apply { eventId?.let { parentEventId = eventId.toProto() } }
        ).build()

        return this
    }

    override fun build(): IGroupBatch = ProtoBatchWrapper(
        ProtoMessageGroupBatch.newBuilder()
            .addAllGroups(groups.map { ProtoMessageGroup.newBuilder().addAllMessages(it).build() })
            .build()
    )
}