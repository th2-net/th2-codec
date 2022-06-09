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
    private val codecRootEvent: String,
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
            val parentEventIds = if (useParentEventId) messageGroup.allParentEventIds else emptySet()
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

                val eventIds = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, e)

                messageBatch.addGroups(header, messageGroup, parentEventIds, e, eventIds, codecRootEvent)
            } catch (throwable: Throwable) {
                val header = "Failed to decode message group"

                val eventIds = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, throwable)

                messageBatch.addGroups(header, messageGroup, parentEventIds, throwable, eventIds, codecRootEvent)
            }

            parentEventIds.onEachWarning(context, "decoding") { messageGroup.messageIds }
        }

        return messageBatch.build().apply {
            if (source.groupsCount != groupsCount) {
                onErrorEvent("Group count in the decoded batch ($groupsCount) is different from the input one (${source.groupsCount})")
            }
        }
    }

    private fun MessageGroupBatch.Builder.addGroups(header: String,
                                                    messageGroup: MessageGroup,
                                                    parentEventIds: Set<String>,
                                                    throwable: Throwable,
                                                    eventIds: Set<String>,
                                                    codecRootEvent: String) {
        val eventIdsMap: Map<String, EventID> = (parentEventIds zip eventIds).associate {
            it.first to EventID.newBuilder().setId(it.second).build()
        }

        addGroups(messageGroup.toErrorGroup(header, protocols, eventIdsMap, throwable, useParentEventId, codecRootEvent))
    }

    private fun MessageGroup.toErrorGroup(infoMessage: String,
                                          protocols: Collection<String>,
                                          errorEvents: Map<String, EventID>,
                                          throwable: Throwable?,
                                          useParentEventId: Boolean,
                                          codecRootEvent: String): MessageGroup {
        val content = buildString {
            appendLine("Error: $infoMessage")
            appendLine("For messages: [${messageIds.joinToString { it.toDebugString() }}] with protocols: $protocols")
            appendLine("Due to the following errors: ")

            generateSequence(throwable, Throwable::cause).forEachIndexed { index, cause ->
                appendLine("$index: ${cause.message}")
            }
        }

        return MessageGroup.newBuilder().also { result ->
            for (anyMessage in this.messagesList) {
                if (anyMessage.hasRawMessage() && anyMessage.rawMessage.metadata.protocol.run { isBlank() || this in protocols }) {
                    result += anyMessage.rawMessage.let { rawMessage ->
                        val eventID: EventID = if (useParentEventId) {
                            checkNotNull(errorEvents[rawMessage.parentEventId.id.ifEmpty { null }]) {
                                "No error event was found for message: ${rawMessage.metadata.id.sequence}"
                            }
                        } else {
                            if (parentEventId != null) {
                                EventID.newBuilder().setId(parentEventId).build()
                            } else {
                                EventID.newBuilder().setId(codecRootEvent.onErrorEvent("")).build()
                            }
                        }

                        rawMessage.toErrorMessage(protocols, eventID, content)
                    }
                } else {
                    result.addMessages(anyMessage)
                }
            }
        }.build()
    }
}