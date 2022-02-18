package com.exactpro.th2.codec.exception.process

class DecodeException : ProcessException {
    constructor(message: String = EXCEPTION_HEADER) : super(message)
    constructor(cause: Throwable) : super(EXCEPTION_HEADER, cause = cause)

    companion object {
        private const val EXCEPTION_HEADER = "Failed to decode message group"
    }
}