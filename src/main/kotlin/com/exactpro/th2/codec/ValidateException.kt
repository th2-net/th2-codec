package com.exactpro.th2.codec

class ValidateException : CodecException {

    var msgType: String? = null
    var tag: Int? = null
    var scenario: String? = null

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

    constructor(msgType: String?, tag: Int?, scenario: String?, cause: Throwable?) : super(cause) {
        this.msgType = msgType
        this.tag = tag
        this.scenario = scenario
    }
}