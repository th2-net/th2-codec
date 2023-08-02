/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.configuration.TransportType.PROTOBUF
import com.exactpro.th2.codec.configuration.TransportType.TH2_TRANSPORT
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.codec.util.load
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

internal val OBJECT_MAPPER: ObjectMapper = ObjectMapper(JsonFactory()).apply {
    registerKotlinModule()
    registerModule(SimpleModule().addAbstractTypeMapping(IPipelineCodecSettings::class.java, load<IPipelineCodecFactory>().settingsClass))
    configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
}

class Configuration {
    var codecSettings: IPipelineCodecSettings? = null
    var enableVerticalScaling: Boolean = false
    var transportLines: Map<String, TransportLine> = mapOf(
        "" to TransportLine(PROTOBUF, false),
        "general" to TransportLine(PROTOBUF, true),
        "transport" to TransportLine(TH2_TRANSPORT, false),
        "general_transport" to TransportLine(TH2_TRANSPORT, true)
    )

    companion object {
        fun create(commonFactory: CommonFactory): Configuration =
            commonFactory.getCustomConfiguration(Configuration::class.java, OBJECT_MAPPER)
    }
}

enum class TransportType {
    PROTOBUF,
    TH2_TRANSPORT
}

class TransportLine(val type: TransportType, val useParentEventId: Boolean)