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
import com.exactpro.th2.codec.util.allParsedProtocols
import com.exactpro.th2.codec.util.messageIds
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class EncodeProcessor(
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
            onEvent("Group count in the encoded batch (${resultGroups.size}) is different from the input one (${sourceGroups.size})")
        }

        return DemoGroupBatch(
            book = source.book,
            sessionGroup = source.sessionGroup,
            groups = resultGroups
        )
    }

    private fun processMessageGroupAsync(group: DemoMessageGroup): CompletableFuture<DemoMessageGroup?> = CompletableFuture.supplyAsync { processMessageGroup(group) }

    private fun processMessageGroup(messageGroup: DemoMessageGroup): DemoMessageGroup? {
        val messages = messageGroup.messages

        if (messages.isEmpty()) {
            onErrorEvent("Cannot encode empty message group")
            return null
        }

        if (messages.none { it is DemoParsedMessage }) {
            logger.debug { "Message group has no parsed messages in it" }
            return messageGroup
        }

        val msgProtocols = messageGroup.allParsedProtocols
        val parentEventIds: Sequence<EventID> = if (useParentEventId) messageGroup.allParentEventIds else emptySequence()
        val context = ReportingContext()

        try {
            if (!protocols.checkAgainstProtocols(msgProtocols)) {
                logger.debug { "Messages with $msgProtocols protocols instead of $protocols are presented" }
                return messageGroup
            }

            val encodedGroup = codec.encode(messageGroup, context)

            if (encodedGroup.messages.size > messages.size) {
                parentEventIds.onEachEvent("Encoded message group contains more messages (${encodedGroup.messages.size}) than decoded one (${messages.size})")
            }

            return encodedGroup
        } catch (e: ValidateException) {
            sendErrorEvents("Failed to encode: ${e.title}", parentEventIds, messageGroup, e, e.details)
            return null
        } catch (throwable: Throwable) {
            // we should not use message IDs because during encoding there is no correct message ID created yet
            sendErrorEvents("Failed to encode message group", parentEventIds, messageGroup, throwable, emptyList())
            return null
        } finally {
            parentEventIds.onEachWarning(context, "encoding",
                additionalBody = { messageGroup.toReadableBody() })
        }
    }

    private fun DemoMessageGroup.toReadableBody(): List<String> = messages.map(DemoMessage<*>::toJson)

    private fun sendErrorEvents(
        errorMsg: String, parentEventIds: Sequence<EventID>, msgGroup: DemoMessageGroup,
        cause: Throwable, additionalBody: List<String>,
    ) {
        parentEventIds.onEachErrorEvent(errorMsg, msgGroup.messageIds, cause, additionalBody + msgGroup.toReadableBody())
    }
}