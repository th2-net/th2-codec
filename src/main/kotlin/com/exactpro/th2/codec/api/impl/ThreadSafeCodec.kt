/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class ThreadSafeCodec(
    private val codecFactory: IPipelineCodecFactory,
    private val codecSettings: IPipelineCodecSettings?
) : IPipelineCodec {
    private val instances = ConcurrentHashMap<Long, IPipelineCodec>()

    override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = getInstance().let { codec ->
        synchronized(codec) {
            codec.encode(messageGroup, context)
        }
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = getInstance().let { codec ->
        synchronized(codec) {
            codec.decode(messageGroup, context)
        }
    }

    override fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = getInstance().let { codec ->
        synchronized(codec) { codec.encode(messageGroup, context) }
    }

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = getInstance().let { codec ->
        synchronized(codec) { codec.decode(messageGroup, context) }
    }

    private fun getInstance() = instances.computeIfAbsent(Thread.currentThread().id) {
        synchronized(codecFactory) { codecFactory.create(codecSettings) }
    }

    override fun close() {
        synchronized(codecFactory) {
            instances.values.forEach { codec ->
                synchronized(codec, codec::close)
            }

            codecFactory.close()
        }
    }
}