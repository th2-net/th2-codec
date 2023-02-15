/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.api.impl

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.common.grpc.MessageGroup
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.TimeUnit.SECONDS
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class ThreadSafeCodec(
    private val codecFactory: IPipelineCodecFactory,
    private val codecSettings: IPipelineCodecSettings?
) : IPipelineCodec {
    private val instances = ConcurrentHashMap<Thread, IPipelineCodec>()

    private val executor = Executors.newSingleThreadScheduledExecutor().apply {
        scheduleAtFixedRate(::cleanup, 1, 1, MINUTES)
    }

    override fun encode(messageGroup: MessageGroup) = getInstance().let { codec ->
        synchronized(codec) {
            codec.encode(messageGroup)
        }
    }

    override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = getInstance().let { codec ->
        synchronized(codec) {
            codec.encode(messageGroup, context)
        }
    }

    override fun decode(messageGroup: MessageGroup) = getInstance().let { codec ->
        synchronized(codec) {
            codec.decode(messageGroup)
        }
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = getInstance().let { codec ->
        synchronized(codec) {
            codec.decode(messageGroup, context)
        }
    }

    private fun getInstance() = instances.computeIfAbsent(Thread.currentThread()) {
        synchronized(codecFactory) { codecFactory.create(codecSettings) }
    }

    private fun cleanup() = instances.entries.removeIf { (thread, instance) ->
        if (thread.isAlive) return@removeIf false

        instance.runCatching(IPipelineCodec::close).onFailure {
            K_LOGGER.error(it) { "Failed to close codec instance" }
        }

        true
    }

    override fun close() {
        executor.shutdown()

        if (!executor.awaitTermination(5, SECONDS)) {
            executor.shutdownNow().also {
                K_LOGGER.warn { "Executor can not be cloded via 5 seconds, ${it.size} tasks aren't completed" }
            }
        }

        synchronized(codecFactory) {
            instances.values.forEach { codec ->
                synchronized(codec, codec::close)
            }

            codecFactory.close()
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}