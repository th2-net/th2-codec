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

package com.exactpro.th2.codec.util

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID

val MessageGroup.parentEventId: String?
    get() = messagesList.firstNotNullOfOrNull { anyMessage ->
        when {
            anyMessage.hasMessage() -> anyMessage.message.parentEventId.id.ifEmpty { null }
            anyMessage.hasRawMessage() -> anyMessage.rawMessage.parentEventId.id.ifEmpty { null }
            else -> null
        }
    }

val MessageGroup.allParentEventIds: Set<String?>
    get() = messagesList.mapTo(HashSet()) { anyMessage ->
        when {
            anyMessage.hasMessage() -> anyMessage.message.parentEventId.id.ifEmpty { null }
            anyMessage.hasRawMessage() -> anyMessage.rawMessage.parentEventId.id.ifEmpty { null }
            else -> null
        }
    }

val MessageGroup.allRawProtocols
    get() = messagesList.asSequence()
        .filter(AnyMessage::hasRawMessage)
        .map { it.rawMessage.metadata.protocol }
        .toSet()

val MessageGroup.allParsedProtocols
    get() = messagesList.asSequence()
        .filter(AnyMessage::hasMessage)
        .map { it.message.metadata.protocol }
        .toSet()


val MessageGroup.messageIds: List<MessageID>
    get() = messagesList.map { message ->
        when (val kind = message.kindCase) {
            MESSAGE -> message.message.metadata.id
            RAW_MESSAGE -> message.rawMessage.metadata.id
            else -> error("Unknown message kind: $kind")
        }
    }
