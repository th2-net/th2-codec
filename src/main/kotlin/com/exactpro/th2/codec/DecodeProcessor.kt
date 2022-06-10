/*
 *  Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.codec.util.*
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import mu.KotlinLogging

class DecodeProcessor(
    codec: IPipelineCodec,
    private val protocols: Set<String>,
    private val useParentEventId: Boolean = true,
    onEvent: (event: Event, parentId: String?) -> Unit
) : AbstractCodecProcessor(codec, onEvent) {

    private val logger = KotlinLogging.logger {}

    override fun process(source: MessageGroupBatch): MessageGroupBatch {
        val messageBatch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

        for (messageGroup in source.groupsList) {
            if (messageGroup.messagesCount == 0) {
                onErrorEvent("Cannot decode empty message group")
                continue
            }

            if (messageGroup.messagesList.none(AnyMessage::hasRawMessage)) {
                logger.debug { "Message group has no raw messages in it" }
                messageBatch.addGroups(messageGroup)
                continue
            }

            val msgProtocols = messageGroup.allRawProtocols
            val parentEventIds: Set<String?> = if (useParentEventId) messageGroup.allParentEventIds.ifEmpty { ROOT_EVENT_ID_SET } else ROOT_EVENT_ID_SET
            val context = ReportingContext()

            try {
                if (!protocols.checkAgainstProtocols(msgProtocols)) {
                    logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                    messageBatch.addGroups(messageGroup)
                    continue
                }

                val decodedGroup = codec.decode(messageGroup, context)

                if (decodedGroup.messagesCount < messageGroup.messagesCount) {
                    parentEventIds.onEachEvent("Decoded message group contains less messages (${decodedGroup.messagesCount}) than encoded one (${messageGroup.messagesCount})")
                }

                messageBatch.addGroups(decodedGroup)
            } catch (e: ValidateException) {
                val header = "Failed to decode: ${e.title}"

                val map = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, e)
                messageBatch.addGroups(messageGroup.toErrorGroup(header, protocols, e, map))
            } catch (throwable: Throwable) {
                val header = "Failed to decode message group"

                val map = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, throwable)
                messageBatch.addGroups(messageGroup.toErrorGroup(header, protocols, throwable, map))
            }

            parentEventIds.onEachWarning(context, "decoding") { messageGroup.messageIds }
        }

        return messageBatch.build().apply {
            if (source.groupsCount != groupsCount) {
                onErrorEvent("Group count in the decoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    private fun MessageGroup.toErrorGroup(
        infoMessage: String,
        protocols: Collection<String>,
        throwable: Throwable,
        map: Map<String?, EventID>
    ): MessageGroup {
        val content = buildString {
            appendLine("Error: $infoMessage")
            appendLine("For messages: [${messageIds.joinToString { it.toDebugString() }}] with protocols: $protocols")
            appendLine("Due to the following errors: ")

            generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
                appendLine("$index: ${cause.message}")
            }
        }

        return MessageGroup.newBuilder().also { batchBuilder ->
            for (anyMessage in this.messagesList) {
                if (anyMessage.hasRawMessage() && anyMessage.rawMessage.metadata.protocol.run { isBlank() || this in protocols } ) {

                    val originEventId = if (useParentEventId) anyMessage.rawMessage.parentEventId.id.ifEmpty { null } else null
                    val eventID = checkNotNull(map[originEventId]) {
                        "Not found id: ${anyMessage.rawMessage.parentEventId.id}\n" +
                                "Map: ${map.entries.joinToString { e -> "${e.key}:${e.value}" }}" +
                                "No error event was found for message: ${anyMessage.rawMessage.metadata.id.sequence}"
                    }

                    batchBuilder += anyMessage.rawMessage.toErrorMessage(protocols, eventID, content)
                } else {
                    batchBuilder.addMessages(anyMessage)
                }
            }
        }.build()
    }

    companion object {
        private val ROOT_EVENT_ID_SET = setOf<String?>(null)
    }
}