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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toTimestamp
import java.time.Instant

fun Message.Builder.setProtocol(protocol: String) = apply {
    metadataBuilder.protocol = protocol
}

fun RawMessage.Builder.setProtocol(protocol: String) = apply {
    metadataBuilder.protocol = protocol
}

const val BOOK_NAME = "test-book"

val CODEC_EVENT_ID: EventID = EventID.newBuilder().apply {
    bookName = BOOK_NAME
    scope = "test-scope"
    startTimestamp = Instant.now().toTimestamp()
    id = "test-codec"
}.build()

val MESSAGE_ID: MessageID = MessageID.newBuilder().apply {
    connectionIdBuilder.apply {
        sessionAlias = "test-session-alias"
    }
    bookName = BOOK_NAME
    direction = Direction.FIRST
    timestamp = Instant.now().toTimestamp()
    sequence = 1
}.build()