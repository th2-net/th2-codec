package com.exactpro.th2.codec.api.impl

import com.exactpro.th2.codec.api.DictionaryAlias
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import java.io.InputStream

class PipelineCodecContext(private val commonFactory: CommonFactory) : IPipelineCodecContext {
    @Deprecated("Dictionary types will be removed in future releases of infra", ReplaceWith("getByAlias(alias)"))
    override fun get(type: DictionaryType): InputStream = commonFactory.readDictionary(type)
    override fun get(alias: DictionaryAlias): InputStream = commonFactory.loadDictionary(alias)
    override fun getDictionaryAliases(): Set<String> = commonFactory.dictionaryAliases
}