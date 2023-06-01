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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import java.time.Instant

const val BOOK_NAME = "test-book"
const val SESSION_GROUP_NAME = "test-session-group"

val CODEC_EVENT_ID: EventId = EventId(
    "test-codec",
    BOOK_NAME,
    "test-scope",
    Instant.now()
)

val MESSAGE_ID: MessageId = MessageId(
    "test-session-alias",
    Direction.INCOMING,
    1,
    Instant.now()
)