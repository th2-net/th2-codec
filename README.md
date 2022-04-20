# Description (4.7.0)

This is a common codec library which takes care of some boilerplate stuff like subscribing/publishing to message queues, loading codec settings, etc.

# What is a codec?

The codec in th2 is a component that is responsible for transforming messages from human-readable format
into a format of a corresponding protocol and vice versa.
It contains the main logic for encoding and decoding messages.

The codec communicates with other components by sending batches with groups of parsed or/and raw messages.
During encoding, it transforms messages to the corresponding protocol format.
During decoding, it takes all raw messages that correspond to the codec protocol and transforms them according to its rules.

Several codecs can be joined into a chain of codecs to reuse already implemented codecs. For example, you have **HTTP**, **JSON** and **XML** codec.
You can join them together for decoding **XML** over **HTTP** or **JSON** over **HTTP**.

Here is a schema that illustrates the common place of the th2-codec component in th2.

![](doc/img/codec-place-in-th2.svg "Place of th2-codec in th2 schema")

# How to create your own codec?

To implement a codec using this library you need to:

1. add following repositories into `build.gradle`:

    ```groovy
    maven {
        url 'https://s01.oss.sonatype.org/content/repositories/snapshots/'
    }
    
    maven {
        url 'https://s01.oss.sonatype.org/content/repositories/releases/'
    }
    ```

2. add dependency on `com.exactpro.th2:codec:4.6.0` into `build.gradle`

3. set main class to `com.exactpro.th2.codec.MainKt`

   > This is usually done by using Gradle's [application](https://docs.gradle.org/current/userguide/application_plugin.html) plugin where you can set main class like this:
   >```groovy
   >application {
   >    mainClassName 'com.exactpro.th2.codec.MainKt'
   >}
   >```

4. implement the codec itself by implementing [`IPipelineCodec`](https://github.com/th2-net/th2-codec/blob/2707a2755038d49110f6f7eb3e3aeb6188ae0c99/src/main/kotlin/com/exactpro/th2/codec/api/IPipelineCodec.kt#L21) interface:
    ```kotlin
    interface IPipelineCodec : AutoCloseable {
        fun encode(messageGroup: MessageGroup): MessageGroup = TODO("encode(messageGroup) method is not implemented")
        fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = encode(messageGroup)

        fun decode(messageGroup: MessageGroup): MessageGroup = TODO("decode(messageGroup) method is not implemented")
        fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup = decode(messageGroup)
        override fun close() {}
    }
    ```

5. implement a factory for it using [`IPipelineCodecFactory`](https://github.com/th2-net/th2-codec/blob/2707a2755038d49110f6f7eb3e3aeb6188ae0c99/src/main/kotlin/com/exactpro/th2/codec/api/IPipelineCodecFactory.kt#L21) interface

    ```kotlin
    interface IPipelineCodecFactory : AutoCloseable {
        val protocols: Set<String>
        val settingsClass: Class<out IPipelineCodecSettings>
        fun init(dictionary: InputStream): Unit = TODO("not implemented")
        fun init(pipelineCodecContext: IPipelineCodecContext): Unit = pipelineCodecContext[DictionaryType.MAIN].use(::init)
        fun create(settings: IPipelineCodecSettings? = null): IPipelineCodec
        override fun close() {}
    }
    ```
   > **NOTE**: both `init` methods have default implementations. One of them **must** be overridden in your factory implementation.
   > If your codec needs the **MAIN** dictionary only you can override the `init(dictionary: InputStream)` method.
   > Otherwise, you should override `init(pipelineCodecContext: IPipelineCodecContext)` method.

   > **IMPORTANT**: implementation should be loadable via Java's built-in [service loader](https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html)

6. Et voilà! Your codec is now complete

# How it works:

Codec operates with [message groups](https://github.com/th2-net/th2-grpc-common/blob/f2794b2c5c8ae945e7500677439809db9c576c43/src/main/proto/th2_grpc_common/common.proto#L97)
whom may contain a mix of [raw](https://github.com/th2-net/th2-grpc-common/blob/f2794b2c5c8ae945e7500677439809db9c576c43/src/main/proto/th2_grpc_common/common.proto#L84)
and [parsed](https://github.com/th2-net/th2-grpc-common/blob/f2794b2c5c8ae945e7500677439809db9c576c43/src/main/proto/th2_grpc_common/common.proto#L78) messages.

## Encoding

During encoding codec must replace each parsed message of supported [protocols](https://github.com/th2-net/th2-grpc-common/blob/f2794b2c5c8ae945e7500677439809db9c576c43/src/main/proto/th2_grpc_common/common.proto#L47)
in a message group with a raw one by encoding parsed message's content

> **NOTE**: codec can merge content of subsequent raw messages into a resulting raw message  
> (e.g. when a codec encodes only a transport layer and its payload is already encoded)

## Decoding

During decoding codec must replace each raw message in a message group with a parsed one by decoding raw message's content. \
If exception was thrown, all raw messages will be replaced with th2-codec-error parsed messages

> **NOTE**: codec can replace raw message with a parsed message followed by several raw messages
> (e.g. when a codec decodes only a transport layer it can produce a parsed message for the transport layer and several raw messages for its payload)

# Configuration

Codec has four types of connection: stream and general for encode and decode functions.

* stream encode / decode connections works 24 / 7
* general encode / decode connections works on demand

Codec never mixes messages from the _stream_ and the _general_ connections

## Codec settings

Codec settings can be specified in `codecSettings` field of `custom-config`. These settings will be loaded as an instance of `IPipelineCodecFactory.settingsClass` during start up and then passed to every invocation
of `IPipelineCodecFactory.create` method

For example:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  custom-config:
    codecSettings:
      messageTypeDetection: BY_INNER_FIELD
      messageTypeField: "messageType"
      rejectUnexpectedFields: true
      treatSimpleValuesAsStrings: false
```

## Required pins

Pins are a part of the main th2 concept. They describe what are the inputs and outputs of a box.
You can read more about them [here](https://github.com/th2-net/th2-documentation/wiki/infra:-Theory-of-Pins-and-Links#pins).

Every type of connection has two `subscribe` and `publish` pins.
The first one is used to receive messages to decode/encode while the second one is used to send decoded/encoded messages further.
**Configuration should include at least one pin for each of the following sets of attributes:**
+ Pin for the stream encoding input: `encoder_in` `parsed` `subscribe`
+ Pin for the stream encoding output: `encoder_out` `raw` `publish`
+ Pin for the general encoding input: `general_encoder_in` `parsed` `subscribe`
+ Pin for the general encoding output: `general_encoder_out` `raw` `publish`
+ Pin for the stream decoding input: `decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `decoder_out` `parsed` `publish`
+ Pin for the stream decoding input: `general_decoder_in` `raw` `subscribe`
+ Pin for the stream decoding output: `general_decoder_out` `parsed` `publish`

### Configuration example

This configuration is a general way for deploying components in th2.
It contains box configuration, pins' descriptions and other common parameters for a box.

Here is an example of configuration for component based on th2-codec:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  custom-config:
    codecSettings:
      parameter1: value
      parameter2:
        - value1
        - value2
  pins:
    # encoder
    - name: in_codec_encode
      connection-type: mq
      attributes: [ 'encoder_in', 'parsed', 'subscribe' ]
    - name: out_codec_encode
      connection-type: mq
      attributes: [ 'encoder_out', 'raw', 'publish' ]
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes: ['decoder_in', 'raw', 'subscribe']
    - name: out_codec_decode
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish']
    # encoder general (technical)
    - name: in_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_in', 'parsed', 'subscribe']
    - name: out_codec_general_encode
      connection-type: mq
      attributes: ['general_encoder_out', 'raw', 'publish']
    # decoder general (technical)
    - name: in_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_in', 'raw', 'subscribe']
    - name: out_codec_general_decode
      connection-type: mq
      attributes: ['general_decoder_out', 'parsed', 'publish']
```

## Message routing

Schema API allows configuring routing streams of messages via links between connections and filters on pins.
Let's consider some examples of routing in codec box.

### Split on 'publish' pins

For example, you got a big source data stream, and you want to split them into some pins via session alias.
You can declare multiple pins with attributes `['decoder_out', 'parsed', 'publish']` and filters instead of common pin or in addition to it.
Every decoded messages will be direct to all declared pins and will send to MQ only if it passes the filter.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec
spec:
  pins:
    # decoder
    - name: out_codec_decode_first_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: first_session_alias
              operation: EQUAL
    - name: out_codec_decode_secon_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: second_session_alias
              operation: EQUAL
```

The filtering can also be applied for pins with `subscribe` attribute.

## Changelog

### v4.7.0

#### Fixed:

* Error logs and error events are made more informative (added custom Exception for validating incoming messages)

### v4.6.1

#### Fixed:

* Codec continued to work when implementation instance cannot be created

### v4.6.0

#### Fixed:

* Errors and warnings during encoding does not have message IDs attached because the IDs are not correct yet

#### Added:

* Codec can report warnings during decoding and encoding message groups

#### Changed:

* Root codec event's name now uses box name
* The general encode/decode does not use `parentEventId` from messages when reporting errors and warnings
* The error/warning events are now attached to the root codec event.
* The error/warning event is attached to the event that is specified in `parentEventId` as a reference to an event in codec root.

### v4.5.0

#### Feature:

* Ability to read more than one dictionary from box configuration in PipelineCodecFactory
* Pipeline codec implementations can declare several protocols to process, not just one  
* Transfers already processed groups through codec without changes,
  for example, encoder transfers groups with raw messages only and vice versa 

### v4.4.0

#### Feature:

* In group required to have all messages (raw messages for decode and parsed for encode) with empty protocol or all filled 
* Failed protocol assertion produce error message in decode processor

### v4.3.0

#### Feature:

* Error event will be sent for each original event id of the message group
* Common version update to 3.32.0
* bom version update to 3.1.0

### v4.2.0

#### Feature:

* In case of decoding error, instead of skipping the group, replace raw messages of empty or target protocol with `th2-codec-error` message in them

### v4.1.1

#### Fixed:

* incorrect protocol checking during encoding
