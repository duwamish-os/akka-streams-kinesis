# Akka Streams Kinesis

[![Build Status](https://travis-ci.org/timeoutdigital/akka-streams-kinesis.svg?branch=master)](https://travis-ci.org/timeoutdigital/akka-streams-kinesis)

This library allows you to read from and write to Kinesis with Akka Streams. It is currently under active development.

## Reading from Kinesis

You can read from Kinesis with `KinesisSource`. It currently polls kinesis every 100ms with the Java async client.

```scala
import com.timeout.KinesisSource
import java.time.ZonedDateTime

val since = ZonedDateTime.now() // from when to start reading the stream
val stage = KinesisSource("my-stream-name", since = since)
```

⚠️ Currently not supported:

 - Shard iterator types other than `AT_TIMESTAMP`
 - More than 5 minutes of backpressure 
 - [Resharding of the stream](http://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html)

 
## Writing to Kinesis

You can write to Kinesis with `KinesisGraphStage`. It maintains an internal buffer of records which it flushes periodically to Kinesis. and emits a stream of  `Either[PutRecordsResultEntry, A]` where the left is any failed results from kinesis and the right is the records pushed successfully.

KinesisGraphStage expects you to implement a typeclass `ToPutRecordsRequest[A]` which tells it how to convert the contents of your stream into a `PutRecordsRequest`.

```scala
import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.timeout.{KinesisGraphStage, ToPutRecordsRequest}
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

implicit val instance = ToPutRecordsRequest.instance[String] { str =>
  new PutRecordsRequestEntry().withData(ByteBuffer.wrap(str.getBytes))
}

val kinesis = new AmazonKinesisAsyncClient()
val stage = KinesisGraphStage.withClient[String](kinesis, "my-stream")
// stage: Flow[String, Either[PutRecordsResultEntry,String], NotUsed]
```
