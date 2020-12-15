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

import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.api.IPipelineCodecSettingsTypeProvider
import com.exactpro.th2.codec.util.load
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.File

internal val OBJECT_MAPPER: ObjectMapper = ObjectMapper(YAMLFactory()).apply {
    registerKotlinModule()
    registerModule(SimpleModule().addAbstractTypeMapping(IPipelineCodecSettings::class.java, load<IPipelineCodecSettingsTypeProvider>().type))
    configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
}

class Configuration {
    var codecSettings: IPipelineCodecSettings? = null

    companion object {
        const val DECODER_INPUT_ATTRIBUTE: String = "decoder_in"
        const val DECODER_OUTPUT_ATTRIBUTE: String = "decoder_out"
        const val ENCODER_INPUT_ATTRIBUTE: String = "encoder_in"
        const val ENCODER_OUTPUT_ATTRIBUTE: String = "encoder_out"
        const val GENERAL_DECODER_INPUT_ATTRIBUTE: String = "general_decoder_in"
        const val GENERAL_DECODER_OUTPUT_ATTRIBUTE: String = "general_decoder_out"
        const val GENERAL_ENCODER_INPUT_ATTRIBUTE: String = "general_encoder_in"
        const val GENERAL_ENCODER_OUTPUT_ATTRIBUTE: String = "general_encoder_out"

        fun create(commonFactory: CommonFactory, settingsPath: String?): Configuration {
            return commonFactory.getCustomConfiguration(Configuration::class.java, OBJECT_MAPPER).apply {
                codecSettings = codecSettings ?: settingsPath?.run {
                    check(isNotBlank()) { "Path to codec settings file is empty" }

                    val type = load<IPipelineCodecSettingsTypeProvider>().type
                    val file = File(this)

                    check(file.isFile) { "Path to codec settings does not exist or is not a file: ${file.canonicalPath}" }

                    file.inputStream().use {
                        runCatching { OBJECT_MAPPER.readValue(it, type) }.getOrElse {
                            throw ConfigurationException("Failed to parse codec settings from file: ${file.canonicalPath}", it)
                        }
                    }
                }
            }
        }
    }
}
