/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.MessageGroupBatch
import mu.KotlinLogging

abstract class AbstractCodecProcessor(
    protected val codec: IPipelineCodec,
    private val onEvent: (name: String, type: String, cause: Throwable?) -> Unit
) : MessageProcessor<MessageGroupBatch, MessageGroupBatch> {
    private val logger = KotlinLogging.logger {}

    protected fun onEvent(message: String, cause: Throwable? = null) = when (cause) {
        null -> {
            onEvent(message, "Warn", null)
            logger.warn { message }
        }
        else -> {
            onEvent(message, "Error", cause)
            logger.error(cause) { message }
        }
    }
}