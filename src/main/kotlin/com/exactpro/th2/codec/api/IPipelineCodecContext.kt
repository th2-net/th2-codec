package com.exactpro.th2.codec.api

import com.exactpro.th2.common.schema.dictionary.DictionaryType
import java.io.InputStream

fun interface IPipelineCodecContext {
    operator fun get(type: DictionaryType): InputStream
}