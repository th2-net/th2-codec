package com.exactpro.th2.codec.api.impl

import com.exactpro.th2.codec.api.IPipelineCodecContext
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import java.io.InputStream

class PipelineCodecContext(private val commonFactory: CommonFactory) : IPipelineCodecContext {
    override fun getByType(type: DictionaryType): InputStream = type.run(commonFactory::readDictionary)

    override fun getByAlias(alias: String): InputStream = alias.run(commonFactory::loadDictionary)

    override fun getDictionaryAliases(): Set<String> = commonFactory.loadDictionaryAliases()
}