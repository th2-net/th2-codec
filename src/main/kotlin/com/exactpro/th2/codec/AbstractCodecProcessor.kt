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