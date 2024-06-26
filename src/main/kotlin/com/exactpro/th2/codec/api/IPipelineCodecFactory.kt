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

package com.exactpro.th2.codec.api

import com.exactpro.th2.common.schema.dictionary.DictionaryType
import java.io.InputStream

interface IPipelineCodecFactory : AutoCloseable {
    val protocols: Set<String>
    val settingsClass: Class<out IPipelineCodecSettings>
    fun init(dictionary: InputStream): Unit = TODO("init(dictionary) method is not implemented")
    fun init(pipelineCodecContext: IPipelineCodecContext): Unit = pipelineCodecContext[DictionaryType.MAIN].use(::init)
    fun create(settings: IPipelineCodecSettings? = null): IPipelineCodec
    override fun close() {}
}
