/*
 *  Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.util.allParentEventIds
import com.exactpro.th2.codec.util.allRawProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toErrorGroup
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class DecodeProcessor(
    codec: IPipelineCodec,
    private val protocols: Set<String>,
    codecEventID: EventID,
    private val useParentEventId: Boolean = true,
    enabledVerticalScaling: Boolean = false,
    onEvent: (event: ProtoEvent) -> Unit
) : AbstractCodecProcessor(codec, codecEventID, onEvent) {
    private val async = enabledVerticalScaling && Runtime.getRuntime().availableProcessors() > 1

    private val logger = KotlinLogging.logger {}

    override fun process(source: DemoGroupBatch): DemoGroupBatch {
        val sourceGroups = source.groups
        val resultGroups = mutableListOf<DemoMessageGroup>()

        if (async) {
            val messageGroupFutures = Array(sourceGroups.size) {
                processMessageGroupAsync(sourceGroups[it])
            }

            CompletableFuture.allOf(*messageGroupFutures).whenComplete { _, _ ->
                messageGroupFutures.forEach { it.get()?.run(resultGroups::add) }
            }.get()
        } else {
            sourceGroups.forEach { group ->
                processMessageGroup(group)?.run(resultGroups::add)
            }
        }

        if (sourceGroups.size != resultGroups.size) {
            onErrorEvent("Group count in the decoded batch (${resultGroups.size}) is different from the input one (${sourceGroups.size})")
        }

        return DemoGroupBatch(
            book = source.book,
            sessionGroup = source.sessionGroup,
            groups = resultGroups
        )
    }

    private fun processMessageGroupAsync(group: DemoMessageGroup) = CompletableFuture.supplyAsync { processMessageGroup(group) }

    private fun processMessageGroup(messageGroup: DemoMessageGroup): DemoMessageGroup? {
        val messages = messageGroup.messages

        if (messages.isEmpty()) {
            onErrorEvent("Cannot decode empty message group")
            return null
        }

        if (messages.none { it is DemoRawMessage }) {
            logger.debug { "Message group has no raw messages in it" }
            return messageGroup
        }

        val msgProtocols = messageGroup.allRawProtocols
        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.allParentEventIds else emptySequence()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val decodedGroup = codec.decode(messageGroup, context)

            if (decodedGroup.messages.size < messages.size) {
                parentEventIds.onEachEvent("Decoded message group contains less messages (${decodedGroup.messages.size}) than encoded one (${messages.size})")
            }

            return decodedGroup
        } catch (e: ValidateException) {
            val header = "Failed to decode: ${e.title}"

            val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, e)
            return messageGroup.toErrorGroup(header, protocols, e, errorEventId)
        } catch (throwable: Throwable) {
            val header = "Failed to decode message group"

            val errorEventId = parentEventIds.onEachErrorEvent(header, messageGroup.messageIds, throwable)
            return messageGroup.toErrorGroup(header, protocols, throwable, errorEventId)
        } finally {
            parentEventIds.onEachWarning(context, "decoding") { messageGroup.messageIds }
        }
    }
}