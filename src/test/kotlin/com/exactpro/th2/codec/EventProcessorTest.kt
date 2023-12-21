/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.codec

import com.exactpro.th2.codec.EventProcessor.Companion.cradleString
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.codec.util.toProto
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.utils.message.transport.toProto
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.contains
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isSameInstanceAs
import strikt.assertions.withElementAt
import java.time.Instant

class EventProcessorTest {
    private val onEvent: (event: ProtoEvent) -> Unit = mock {  }

    private val eventProcessor = EventProcessor(
        BOOK_NAME_A,
        COMPONENT_NAME,
        onEvent
    )

    @BeforeEach
    fun beforeEach() {
        val captor = argumentCaptor<ProtoEvent> { }
        verify(onEvent).invoke(captor.capture())
        expectThat(captor.allValues.single()) {
            isRootEvent(BOOK_NAME_A, COMPONENT_NAME)
        }
        clearInvocations(onEvent)
    }

    @Test
    fun `onEachEvent with book A,B events, book A,B messages test`() {
        val eventIdA = createEventId(BOOK_NAME_A, "test-scope-a")
        val eventIdB = createEventId(BOOK_NAME_B, "test-scope-b")
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        eventProcessor.onEachEvent(
            mapOf(
                messageIdA to eventIdA,
                messageIdB to eventIdB,
            ),
            "test-message",
            listOf("test-body-a", "test-body-b")
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(5)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            withElementAt(1) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventProcessor.codecEventID.bookName)
                    get { scope }.isEqualTo(eventProcessor.codecEventID.scope)
                }
                get { parentId }.isEqualTo(eventProcessor.codecEventID)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdA)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(2) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventIdA.bookName)
                    get { scope }.isEqualTo(eventIdA.scope)
                }
                get { parentId }.isEqualTo(eventIdA)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdA)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[1].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(3) {
                get { id }.apply {
                    get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                    get { scope }.isEqualTo(captor.allValues[0].id.scope)
                }
                get { parentId }.isEqualTo(captor.allValues[0].id)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(4) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventIdB.bookName)
                    get { scope }.isEqualTo(eventIdB.scope)
                }
                get { parentId }.isEqualTo(eventIdB)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[3].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `onEachEvent with empty events, book A,B messages test`() {
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        eventProcessor.onEachEvent(
            mapOf(
                messageIdA to null,
                messageIdB to null,
            ),
            "test-message",
            listOf("test-body-a", "test-body-b")
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(3)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            withElementAt(1) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventProcessor.codecEventID.bookName)
                    get { scope }.isEqualTo(eventProcessor.codecEventID.scope)
                }
                get { parentId }.isEqualTo(eventProcessor.codecEventID)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdA)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(2) {
                get { id }.apply {
                    get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                    get { scope }.isEqualTo(captor.allValues[0].id.scope)
                }
                get { parentId }.isEqualTo(captor.allValues[0].id)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Warn")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `onEachWarning with book A,B events, book A,B messages test`() {
        val onEvent: (event: ProtoEvent) -> Unit = mock {  }
        val eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent)

        val eventIdA = createEventId(BOOK_NAME_A, "test-scope-a")
        val eventIdB = createEventId(BOOK_NAME_B, "test-scope-b")
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        val context = ReportingContext().apply {
            warning("test-warning-a")
            warning("test-warning-b")
        }

        eventProcessor.onEachWarning(
            mapOf(
                messageIdA to eventIdA,
                messageIdB to eventIdB,
            ),
            context,
            "test-action"
        ) { listOf("test-body-a", "test-body-b") }

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(10)).invoke(captor.capture())
        expectThat(captor.allValues) {
            all {
                get { status }.isEqualTo(EventStatus.SUCCESS)
            }
            withElementAt(0) {
                isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
            }
            withElementAt(1) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            context.warnings.forEachIndexed { index, warning ->
                withElementAt(index * 4 + 2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdA)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(index * 4 + 3) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventIdA.bookName)
                        get { scope }.isEqualTo(eventIdA.scope)
                    }
                    get { parentId }.isEqualTo(eventIdA)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdA)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[index * 4 + 2].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(index * 4 + 4) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[1].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[1].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[1].id)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdB)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(index * 4 + 5) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(eventIdB.bookName)
                        get { scope }.isEqualTo(eventIdB.scope)
                    }
                    get { parentId }.isEqualTo(eventIdB)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdB)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[index * 4 + 4].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
            }
        }
    }

    @Test
    fun `onEachWarning with empty events, book A,B messages test`() {
        val onEvent: (event: ProtoEvent) -> Unit = mock {  }
        val eventProcessor = EventProcessor(BOOK_NAME_A, COMPONENT_NAME, onEvent)

        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        val context = ReportingContext().apply {
            warning("test-warning-a")
            warning("test-warning-b")
        }

        eventProcessor.onEachWarning(
            mapOf(
                messageIdA to null,
                messageIdB to null,
            ),
            context,
            "test-action"
        ) { listOf("test-body-a", "test-body-b") }

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(6)).invoke(captor.capture())
        expectThat(captor.allValues) {
            all {
                get { status }.isEqualTo(EventStatus.SUCCESS)
            }
            withElementAt(0) {
                isRootEvent(BOOK_NAME_A, EVENT_SCOPE)
            }
            withElementAt(1) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            context.warnings.forEachIndexed { index, warning ->
                withElementAt(index * 2 + 2) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[0].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[0].id)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdA)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
                withElementAt(index * 2 + 3) {
                    get { id }.apply {
                        get { bookName }.isEqualTo(captor.allValues[1].id.bookName)
                        get { scope }.isEqualTo(captor.allValues[1].id.scope)
                    }
                    get { parentId }.isEqualTo(captor.allValues[1].id)
                    get { name }.isEqualTo("[WARNING] During test-action: $warning")
                    get { type }.isEqualTo("Warn")
                    get { attachedMessageIdsList }.apply {
                        hasSize(1)
                        contains(messageIdB)
                    }
                    get { String(body.toByteArray()) }.isEqualTo(
                        """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                    )
                }
            }
        }
    }

    @Test
    fun `onErrorEvent with book B event, book A,B messages test`() {
        val eventIdB = createEventId(BOOK_NAME_B, "test-scope-b")
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        eventProcessor.onErrorEvent(
            eventIdB,
            "test-message",
            listOf(messageIdA, messageIdB),
            RuntimeException("test-error")
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(1)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { id }.apply {
                    get { bookName }.isEqualTo(eventIdB.bookName)
                    get { scope }.isEqualTo(eventIdB.scope)
                }
                get { parentId }.isEqualTo(eventIdB)

                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `onErrorEvent with null event, book A,B messages test`() {
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        eventProcessor.onErrorEvent(
            null,
            "test-message",
            listOf(messageIdA, messageIdB),
            RuntimeException("test-error")
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(1)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { id }.apply {
                    get { bookName }.isEqualTo(eventProcessor.codecEventID.bookName)
                    get { scope }.isEqualTo(eventProcessor.codecEventID.scope)
                }
                get { parentId }.isEqualTo(eventProcessor.codecEventID)

                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdA)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `onEachErrorEvent with book A,B events, book B,C messages test`() {
        val eventIdA = createEventId(BOOK_NAME_A, "test-scope-a")
        val eventIdB = createEventId(BOOK_NAME_B, "test-scope-b")
        val messageIdB = createMessageId(BOOK_NAME_B)
        val messageIdC = createMessageId(BOOK_NAME_C)

        eventProcessor.onEachErrorEvent(
            mapOf(
                messageIdB to eventIdA,
                messageIdC to eventIdB,
            ),
            "test-message",
            RuntimeException("test-error"),
            listOf("test-body-a", "test-body-b"),
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(5)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            withElementAt(1) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventProcessor.codecEventID.bookName)
                    get { scope }.isEqualTo(eventProcessor.codecEventID.scope)
                }
                get { parentId }.isEqualTo(eventProcessor.codecEventID)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.isEmpty()
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"messageId":"${messageIdC.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(2) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventIdA.bookName)
                    get { scope }.isEqualTo(eventIdA.scope)
                }
                get { parentId }.isEqualTo(eventIdA)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.isEmpty()
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[1].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"messageId":"${messageIdC.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(3) {
                get { id }.apply {
                    get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                    get { scope }.isEqualTo(captor.allValues[0].id.scope)
                }
                get { parentId }.isEqualTo(captor.allValues[0].id)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdC.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(4) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventIdB.bookName)
                    get { scope }.isEqualTo(eventIdB.scope)
                }
                get { parentId }.isEqualTo(eventIdB)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to the codec event","type":"message"},
                        {"eventId":"${captor.allValues[3].id.cradleString}","type":"reference"},
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdC.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `onEachErrorEvent with empty events, book A,B messages test`() {
        val messageIdA = createMessageId(BOOK_NAME_A)
        val messageIdB = createMessageId(BOOK_NAME_B)

        eventProcessor.onEachErrorEvent(
            mapOf(
                messageIdA to null,
                messageIdB to null,
            ),
            "test-message",
            RuntimeException("test-error"),
            listOf("test-body-a", "test-body-b"),
        )

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(3)).invoke(captor.capture())
        expectThat(captor.allValues) {
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, EVENT_SCOPE)
            }
            withElementAt(1) {
                get { id }.apply {
                    get { bookName }.isEqualTo(eventProcessor.codecEventID.bookName)
                    get { scope }.isEqualTo(eventProcessor.codecEventID.scope)
                }
                get { parentId }.isEqualTo(eventProcessor.codecEventID)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdA)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdB.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
            withElementAt(2) {
                get { id }.apply {
                    get { bookName }.isEqualTo(captor.allValues[0].id.bookName)
                    get { scope }.isEqualTo(captor.allValues[0].id.scope)
                }
                get { parentId }.isEqualTo(captor.allValues[0].id)
                get { name }.isEqualTo("test-message")
                get { type }.isEqualTo("Error")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.apply {
                    hasSize(1)
                    contains(messageIdB)
                }
                get { String(body.toByteArray()) }.isEqualTo(
                    """
                        [
                        {"data":"This event contains reference to messages from another book","type":"message"},
                        {"messageId":"${messageIdA.cradleString}","type":"reference"},
                        {"data":"java.lang.RuntimeException: test-error","type":"message"},
                        {"data":"Information:","type":"message"},
                        {"data":"test-body-a","type":"message"},
                        {"data":"test-body-b","type":"message"}
                        ]
                    """.trimIndent().replace(Regex("\n"), "")
                )
            }
        }
    }

    @Test
    fun `choose events with empty events, empty messages test`() {
        val events = eventProcessor.chooseRootEventsForPublication(emptyMap())
        expectThat(events) {
            hasSize(1)
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_A && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) { hasSize(0) }
            }
        }

        verify(onEvent, never()).invoke(any())
    }

    @Test
    fun `choose events with empty events, book A message test`() {
        val messageId = createMessageId(BOOK_NAME_A)
        val events = eventProcessor.chooseRootEventsForPublication(
            mapOf(
                messageId to null,
            ),
        )
        expectThat(events) {
            hasSize(1)
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_A && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) { hasSize(0) }
            }
        }

        verify(onEvent, never()).invoke(any())
    }

    @Test
    fun `choose events with empty events, book B,C messages test`() {
        val messageIdB = createMessageId(BOOK_NAME_B)
        val messageIdC = createMessageId(BOOK_NAME_C)
        val events = eventProcessor.chooseRootEventsForPublication(
            mapOf(
                messageIdB to null,
                messageIdC to null,
            ),
        )
        expectThat(events) {
            hasSize(2)
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_B && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) { hasSize(0) }
            }
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_C && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) { hasSize(0) }
            }
        }

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(2)).invoke(captor.capture())
        expectThat(captor.allValues) {
            hasSize(2)
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, COMPONENT_NAME)
            }
            withElementAt(1) {
                isRootEvent(BOOK_NAME_C, COMPONENT_NAME)
            }
        }
    }

    @Test
    fun `choose events with book B,C events, book B,C messages test`() {
        val eventIdB = createEventId(BOOK_NAME_B, "test-scope-b")
        val eventIdC = createEventId(BOOK_NAME_C, "test-scope-c")
        val messageIdB = createMessageId(BOOK_NAME_B)
        val messageIdC = createMessageId(BOOK_NAME_C)
        val events = eventProcessor.chooseRootEventsForPublication(
            mapOf(
                messageIdB to eventIdB,
                messageIdC to eventIdC,
            )
        )
        expectThat(events) {
            hasSize(2)
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_B && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) {
                    hasSize(1)
                    withElementAt(0) { isSameInstanceAs(eventIdB) }
                }
            }
            get { entries.asSequence()
                .filter { (key, _) -> key.bookName == BOOK_NAME_C && key.scope == COMPONENT_NAME }
                .map { it.value }
                .toList()
            }.apply {
                hasSize(1)
                withElementAt(0) {
                    hasSize(1)
                    withElementAt(0) { isSameInstanceAs(eventIdC) }
                }
            }
        }

        val captor = argumentCaptor<ProtoEvent> {  }
        verify(onEvent, times(2)).invoke(captor.capture())
        expectThat(captor.allValues) {
            hasSize(2)
            withElementAt(0) {
                isRootEvent(BOOK_NAME_B, COMPONENT_NAME)
            }
            withElementAt(1) {
                isRootEvent(BOOK_NAME_C, COMPONENT_NAME)
            }
        }
    }

    companion object {
        private fun createEventId(book: String, scope: String) = EventId(
            Instant.now().toString(),
            book,
            scope,
            Instant.now()
        ).toProto()

        private fun createMessageId(book: String) = MessageId(
            SESSION_ALIAS_NAME,
            Direction.OUTGOING,
            System.currentTimeMillis(),
            Instant.now(),
        ).toProto(book, SESSION_GROUP_NAME)
    }
}