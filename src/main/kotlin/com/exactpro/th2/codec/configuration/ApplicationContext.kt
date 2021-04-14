/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.messages.structures.loaders.JsonYamlDictionaryStructureLoader
import com.exactpro.sf.common.messages.structures.loaders.XmlDictionaryStructureLoader
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.impl.ThreadSafeCodec
import com.exactpro.th2.codec.util.load
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import mu.KotlinLogging
import java.io.File
import java.nio.file.Files

class ApplicationContext(
    val commonFactory: CommonFactory,
    val codec: IPipelineCodec,
    val eventBatchRouter: MessageRouter<EventBatch>
) : AutoCloseable {
    override fun close() = codec.close()

    companion object {
        private val logger = KotlinLogging.logger { }

        fun create(configuration: Configuration, commonFactory: CommonFactory): ApplicationContext {
            runCatching {
                load<IPipelineCodec>()
            }.onFailure {
                throw IllegalStateException("Failed to load codec", it)
            }

            val eventBatchRouter = commonFactory.eventBatchRouter

            val dictionary = runCatching {
                logger.debug { "Trying to load dictionary as XML" }
                logger.debug { "Configs from default folder /var/th2/configs/:" }
                File("/var/th2/configs/").walk().forEach {
                    logger.debug("${it.name} is regular: ${Files.isRegularFile(it.toPath())}")
                }
                commonFactory.readDictionary(DictionaryType.MAIN).use(XmlDictionaryStructureLoader()::load)
            }.recoverCatching {
                logger.warn(it) { "Failed to load dictionary as XML. Trying to load it as JSON/YAML" }
                commonFactory.readDictionary(DictionaryType.MAIN).use(JsonYamlDictionaryStructureLoader()::load)
            }.getOrThrow()

            val codec = ThreadSafeCodec(dictionary, configuration.codecSettings, ::load)

            return ApplicationContext(
                commonFactory,
                codec,
                eventBatchRouter
            )
        }
    }
}