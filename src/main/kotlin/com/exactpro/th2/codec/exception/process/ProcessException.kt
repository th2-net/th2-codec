package com.exactpro.th2.codec.exception.process

import java.lang.RuntimeException

abstract class ProcessException : RuntimeException {

    constructor() : super()
    constructor(message: String) : super(message)
    constructor(message: String, cause: Throwable) : super(message, cause)
    constructor(cause: Throwable) : super(cause)
    constructor(message: String, cause: Throwable, enableSuppression: Boolean, writableStackTrace: Boolean) : super(
        message,
        cause,
        enableSuppression,
        writableStackTrace
    )

    private val infoItems = mutableListOf<InfoItem>()

    fun addContext(header: String, type: ExceptionType = ExceptionType.ERROR, cause: Throwable? = null) = infoItems.add(InfoItem(header, type, cause))

    val context: List<InfoItem>
        get() = infoItems.toList()



    data class InfoItem(val header: String, val type: ExceptionType, val cause: Throwable?)

    enum class ExceptionType {
        ERROR,
        WARNING
    }
}