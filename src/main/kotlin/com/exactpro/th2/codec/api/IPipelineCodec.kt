/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.api

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup

interface IPipelineCodec : AutoCloseable {
    fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = TODO("encode(messageGroup: MessageGroup, context: IReportingContext) method is not implemented")
    fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = TODO("decode(messageGroup: MessageGroup, context: IReportingContext) method is not implemented")
    fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = TODO("encode(messageGroup: ProtoMessageGroup, context: IReportingContext) method is not implemented")
    fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup = TODO("decode(messageGroup: ProtoMessageGroup, context: IReportingContext) method is not implemented")

    override fun close() {}
}