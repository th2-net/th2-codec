/*
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.codec.filter.*
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext

class DecodeMessageSender(
    context: CoroutineContext,
    codecChannel: Channel<Deferred<MessageBatch>>,
    consumers: List<FilterChannelSender>
) : MessageSender<MessageBatch>(context, codecChannel, consumers) {
    override fun toCommonBatch(codecResult: MessageBatch): CommonBatch  = DecodeCommonBatch(codecResult)
}