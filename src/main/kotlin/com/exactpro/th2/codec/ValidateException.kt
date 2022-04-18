package com.exactpro.th2.codec

class ValidateException : CodecException {

    var title: String? = null

    constructor() : super()
    constructor(title: String?) : super(title) {
        this.title = title
    }

    constructor(cause: Throwable?) : super(cause)
    constructor(title: String?, cause: Throwable?, enableSuppression: Boolean, writableStackTrace: Boolean) : super(
        title,
        cause,
        enableSuppression,
        writableStackTrace
    ) {
        this.title = title
    }

    constructor(title: String?, cause: Throwable?) : super(title, cause) {
        this.title = title
    }
}