package com.exactpro.th2.codec.api.impl

import com.exactpro.th2.codec.api.DictionaryAlias
import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import java.io.File
import java.io.InputStream

class PipelineCodecContext(private val commonFactory: CommonFactory) : IPipelineCodecContext {
    override fun get(type: DictionaryType): InputStream = commonFactory.readDictionary(type)
    override fun get(alias: DictionaryAlias): InputStream = commonFactory.loadDictionary(alias)
    override fun getFile(alias: DictionaryAlias): File = commonFactory.loadDictionaryFile(alias)
    override fun getDictionaryAliases(): Set<String> = commonFactory.dictionaryAliases
}