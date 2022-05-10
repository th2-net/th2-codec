/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.configuration

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.impl.PipelineCodecContext
import com.exactpro.th2.codec.api.impl.ThreadSafeCodec
import com.exactpro.th2.codec.util.load
import com.exactpro.th2.common.schema.factory.CommonFactory
import mu.KotlinLogging

class ApplicationContext(
    val commonFactory: CommonFactory,
    val codec: IPipelineCodec,
    val protocols: Set<String>
) : AutoCloseable {
    override fun close() = codec.close()

    companion object {
        private val logger = KotlinLogging.logger { }

        fun create(configuration: Configuration, commonFactory: CommonFactory): ApplicationContext {
            val factory = runCatching {
                load<IPipelineCodecFactory>().apply {
                    init(PipelineCodecContext(commonFactory))
                }
            }.getOrElse {
                throw IllegalStateException("Failed to load codec factory", it)
            }

            return ApplicationContext(commonFactory, ThreadSafeCodec(factory, configuration.codecSettings), factory.protocols)
        }
    }
}