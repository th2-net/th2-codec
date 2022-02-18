package com.exactpro.th2.codec.exception

import java.lang.RuntimeException

open class CodecException : RuntimeException {
    constructor() : super()
    constructor(message: String?) : super(message)
    constructor(message: String?, cause: Throwable?) : super(message, cause)
    constructor(cause: Throwable?) : super(cause)
    constructor(message: String?, cause: Throwable?, enableSuppression: Boolean, writableStackTrace: Boolean) : super(
        message,
        cause,
        enableSuppression,
        writableStackTrace
    )

    fun getAllMessages(): String = getMessage(this)

    private fun getMessage(exception: Throwable?): String {
        return when {
            exception?.cause != null && exception.cause != exception ->
                "${exception.message}. Caused by: ${getMessage(exception.cause)}"
            exception?.message != null ->
                "${exception.message}."
            else -> ""
        }
    }
}