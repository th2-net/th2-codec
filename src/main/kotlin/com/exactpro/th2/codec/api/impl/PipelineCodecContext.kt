/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.api.DictionaryAlias
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import java.io.InputStream

class PipelineCodecContext(private val commonFactory: CommonFactory) : IPipelineCodecContext {
    @Deprecated("Dictionary types will be removed in future releases of infra", ReplaceWith("getByAlias(alias)"))
    override fun get(type: DictionaryType): InputStream = commonFactory.readDictionary(type)
    override fun get(alias: DictionaryAlias): InputStream = commonFactory.loadDictionary(alias)
    override fun getDictionaryAliases(): Set<String> = commonFactory.dictionaryAliases
    override fun getGrpcRouter(): GrpcRouter = commonFactory.grpcRouter
}