# How it works

The th2 Codec component is responsible for encoding and decoding the messages.
It operates two instances of encoder/decoder pairs, one for operational purposes and one for general conversion.

Encoding and decoding is performed according to the scheme "one or more input pins and one or more output pins".
Both types of pins can have filters, depending on which the input / output of the encoder / decoder can be partially or completely filtered out.
The metadata of the message and its fields can be used as filter parameters.

One instance of the codec implements the logic for encoding and decoding one protocol of one version.
Version-specific protocol messages are described in a separate XML file called a "dictionary".
Codec operates with arrays of messages (parsed batch to raw batch in case of encoding and raw batch to parsed batch upon decoding).

## Appointment

This project includes only adapter logic between Sailfish and th2 packed into Docker Image.
This [th2-codec-generic](https://github.com/th2-net/th2-codec-generic) project uses this image as a base.

# Running

Codec requires a JAR file with external codec API implementation and its dependencies to be place to the `home/codec_implementation` folder.
It loads all JAR files from that directory and looks for all implementations of
[com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt) interface.
After that, it loads the factory defined in the configuration and creates the codec using that factory.

# Creating your own codec

You can create a codec for your protocol by implementing the following interface - [com.exactpro.sf.externalapi.codec.IExternalCodec](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodec.kt).
Also, you will need to implement the [com.exactpro.sf.externalapi.codec.IExternalCodecFactory](https://github.com/exactpro/sailfish-core/blob/master/BackEnd/Core/sailfish-core/src/main/kotlin/com/exactpro/sf/externalapi/codec/IExternalCodecFactory.kt) interface.

The core part of the "Codec" component uses [ServiceLoader](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/ServiceLoader.html) to load all the factory interface implementations.
To provide the ServiceLoader with the knowledge about your factory implementation the JAR file should contain a provider-configuration file named:

**META-INF/services/com.exactpro.sf.externalapi.codec.IExternalCodecFactory**

with the content equals to the fully-qualified class name of your factory implementation.

_If you have several implementations of that interface their fully-qualified names should be written in that file each one on the new line._


# Configuration

Codec has four types of connection: stream and general for encode and decode functions.

* stream encode / decode connections works 24 / 7
* general encode / decode connections works on demand

Codec never mixes messages from the _stream_ and the _general_ connections. 

Decoding can work in two different modes:
+ **CUMULATIVE** (default) - all raw messages in batch will be joined together and decoded. After decoding, content and count of the decoded messages will be compared with the original messages in the batch.
+ **SEQUENTIAL** - each message in the batch will be decoded as separate message.

This setting can be overridden in a custom config for the application using `decodeProcessorType` parameter.

## Bootstrap parameters

These parameters specify the codec to be used for the messages decoding/encoding and the mode which should be used.
Their should be defined in the `custom-config` section of the component configuration.

```yaml
codecClassName: fully.qualified.class.name.for.Factory
decodeProcessorType: CUMULATIVE
```

## Codec implementation parameters

These parameters will be passed to the actual codec implementation to configure its behavior.
A codec might not have any parameters to configure. In that case, you can omit adding those parameters.

The codec implementation parameters should be located in the container's `/home` directory and stored in the file named `codec_config.yml`.
It has simple key-value format in YAML.
```yaml
---
param1: value1
param2: value2
```
The set of parameters depends on the codec implementation is used.

## Required pins

Every type of connection has two `subscribe` and `publish` pins.
The first one is used to receive messages to decode/encode. The second one - to send decoded/encoded messages further.
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
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
metadata:
  name: codec
spec:
  custom-config:
    codecClassName: fully.qualified.class.name.for.Factory
    decodeProcessorType: CUMULATIVE
  pins:
    # encoder
    - name: in_codec_encode
      connection-type: mq
      attributes: ['encoder_in', 'parsed', 'subscribe']
    - name: out_codec_encode
      connection-type: mq
      attributes: ['encoder_out', 'raw', 'publish']
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

For example, you have got big source data stream, and you want to split them on some pins via session alias.
You can declare multiple pins with attributes `['decoder_out', 'parsed', 'publish']` and filters instead of common pin or in addition to it.
Every decoded messages will be direct to all declared pins and will send to MQ only if it passes the filter.

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
metadata:
  name: codec
spec:
  pins:
    # decoder
    - name: out_codec_decode_first_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish', 'first_session_alias']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: first_session_alias
              operation: EQUAL
    - name: out_codec_decode_secon_session_alias
      connection-type: mq
      attributes: ['decoder_out', 'parsed', 'publish', 'second_session_alias']
      filters:
        - metadata:
            - field-name: session_alias
              expected-value: second_session_alias
              operation: EQUAL
```

The filtering can also be performed for pins with  `subscribe` attribute.
