package com.exactpro.th2.codec

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage

fun Message.Builder.setProtocol(protocol: String) = apply {
    metadataBuilder.protocol = protocol
}

fun RawMessage.Builder.setProtocol(protocol: String) = apply {
    metadataBuilder.protocol = protocol
}