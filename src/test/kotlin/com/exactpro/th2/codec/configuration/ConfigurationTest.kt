/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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


package com.exactpro.th2.codec.configuration

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.google.auto.service.AutoService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.absolutePathString
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigurationTest {
    @TempDir
    private lateinit var configDir: Path
    private lateinit var customConfig: Path
    private lateinit var factory: CommonFactory

    @BeforeEach
    fun beforeEach() {
        factory = CommonFactory.createFromArguments("-c", configDir.absolutePathString())
        customConfig = configDir.resolve("custom.json")
    }

    @AfterEach
    fun afterEach() {
        factory.close()
    }

    @Test
    fun `default settings content test`() {
        customConfig.writeText("{}")

        val defaultConfig = Configuration()
        val config = Configuration.create(factory)

        assertEquals(defaultConfig.codecSettings, config.codecSettings)
        assertEquals(defaultConfig.enableVerticalScaling, config.enableVerticalScaling)
        assertEquals(defaultConfig.transportLines, config.transportLines)
        assertEquals(defaultConfig.isFirstCodecInPipeline, config.isFirstCodecInPipeline)
        assertEquals(defaultConfig.disableMessageTypeCheck, config.disableMessageTypeCheck)
        assertEquals(defaultConfig.disableProtocolCheck, config.disableProtocolCheck)
        assertEquals(defaultConfig.eventPublication, config.eventPublication)
    }

    @Test
    fun `ignore additional field test`() { // It is bad practice because user hasn't got feedback from component
        customConfig.writeText("""
            {
              "ignore-additional-field-test": "ignore additional field test",
              "codecSettings": {
                "ignore-additional-field-test": "ignore additional field test"
              }
            }
        """.trimIndent())

        val defaultConfig = Configuration()
        val defaultCodecConfig = TestCodecSettings()
        val config = Configuration.create(factory)

        assertEquals(defaultCodecConfig, config.codecSettings)
        assertEquals(defaultConfig.enableVerticalScaling, config.enableVerticalScaling)
        assertEquals(defaultConfig.transportLines, config.transportLines)
        assertEquals(defaultConfig.isFirstCodecInPipeline, config.isFirstCodecInPipeline)
        assertEquals(defaultConfig.disableMessageTypeCheck, config.disableMessageTypeCheck)
        assertEquals(defaultConfig.disableProtocolCheck, config.disableProtocolCheck)
        assertEquals(defaultConfig.eventPublication, config.eventPublication)
    }

    @Test
    fun `full filled config test`() {
        customConfig.writeText("""
            {
              "codecSettings": {
                "test": "test value"
              },
              "enableVerticalScaling": true,
              "transportLines": {
                "old": {
                  "type": "PROTOBUF",
                  "useParentEventId": true
                },
                "new": {
                  "type": "TH2_TRANSPORT",
                  "useParentEventId": true
                }
              },
              "isFirstCodecInPipeline": true,
              "disableMessageTypeCheck": true,
              "disableProtocolCheck": true,
              "eventPublication": {
                "flushTimeout": 999,
                "batchSize": 99
              }
            }
        """.trimIndent())

        val config = Configuration.create(factory)

        assertEquals(TestCodecSettings("test value"), config.codecSettings)
        assertTrue(config.enableVerticalScaling)
        assertEquals(mapOf(
            "old" to TransportLine(TransportType.PROTOBUF, true),
            "new" to TransportLine(TransportType.TH2_TRANSPORT, true),
        ), config.transportLines)
        assertTrue(config.isFirstCodecInPipeline)
        assertTrue(config.disableMessageTypeCheck)
        assertTrue(config.disableProtocolCheck)
        assertEquals(EventPublicationConfig(999, 99), config.eventPublication)
    }
}

data class TestCodecSettings(
    val test: String = "default-value",
): IPipelineCodecSettings

class TestCodec: IPipelineCodec

@Suppress("unused")
@AutoService(IPipelineCodecFactory::class)
class TestCodecFactory: IPipelineCodecFactory {
    override val protocols: Set<String> = setOf("test")
    override val settingsClass: Class<out IPipelineCodecSettings> = TestCodecSettings::class.java

    override fun create(settings: IPipelineCodecSettings?): IPipelineCodec = TestCodec()
}