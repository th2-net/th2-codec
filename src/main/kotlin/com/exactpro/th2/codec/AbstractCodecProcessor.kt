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
import com.exactpro.th2.common.grpc.MessageGroupBatch

abstract class AbstractCodecProcessor(
    protected val codec: IPipelineCodec,
    protected val eventProcessor: AbstractEventProcessor
) : MessageProcessor<MessageGroupBatch, MessageGroupBatch> {
    protected fun Collection<String>.checkAgainstProtocols(incomingProtocols: Collection<String>) = when {
        incomingProtocols.none { it.isBlank() || containsIgnoreCase(it) } -> false
        incomingProtocols.any(String::isBlank) && incomingProtocols.any(String::isNotBlank) -> error("Mixed empty and non-empty protocols are present. Asserted protocols: $incomingProtocols")
        else -> true
    }

    private fun Collection<String>.containsIgnoreCase(item: String) = any { item.equals(it, true) }
}