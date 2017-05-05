package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime, Duration => JDuration}
import java.util.Date
import java.util.concurrent.{Future => JavaFuture}

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, OutHandler, StageLogging, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object KinesisSource {

  private [timeout] val region: Regions =
    Option(Regions.getCurrentRegion)
      .map(r => Regions.fromName(r.getName))
      .getOrElse(Regions.EU_WEST_1)

  private[timeout] lazy val kinesis: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.standard.withRegion(region).build

  private [timeout] case class ShardIterator(
    iterator: String,
    reissue: GetShardIteratorRequest
  )

  /**
    * Given a response from kinesis and the current shard iterator, prepare a new iterator.
    * The new iterator needs to have a reissue command that can reproduce it if it expires.
    * To do this we either use an AFTER_SEQUENCE_NUMBER request if we have records, or
    * an AT_TIMESTAMP request if we don't
    */
  private [timeout] def nextIterator(
    s: ShardIterator,
    g: GetRecordsResult
  )(implicit c: Clock): ShardIterator = {
    val reissue = g.getRecords.asScala.lastOption.fold {
      val nanosBehind = g.getMillisBehindLatest * 1000
      val ts = ZonedDateTime.now(c).minusNanos(nanosBehind)
      s.reissue
        .withShardIteratorType("AT_TIMESTAMP")
        .withTimestamp(Date.from(ts.toInstant))
    } { lastRecord =>
      s.reissue
        .withShardIteratorType("AFTER_SEQUENCE_NUMBER")
        .withStartingSequenceNumber(lastRecord.getSequenceNumber)
    }
    ShardIterator(g.getNextShardIterator, reissue)
  }

  /**
    * This creates a source that reads records from AWS Kinesis.
    * It is serialisation format agnostic so emits a stream of ByteBuffers
    */
  def apply(
    stream: String,
    since: ZonedDateTime
  )(
    implicit
    ec: ExecutionContext,
    clock: Clock = Clock.systemUTC
  ): Source[ByteBuffer, NotUsed] =
    Source.fromGraph(new KinesisSource(stream, since))

  /**
    * Construct shard iterator requests
    * based on a stream description
    */
  private[timeout] def shardIteratorRequests(
    since: ZonedDateTime,
    stream: StreamDescription
  )(
    implicit
    clock: Clock
  ): List[GetShardIteratorRequest] =
    stream.getShards.asScala.toList.map { shard =>
    val now = clock.instant
    val readFrom = if (since.toInstant.isBefore(now)) since.toInstant else now
    new GetShardIteratorRequest()
      .withShardIteratorType("AT_TIMESTAMP")
      .withTimestamp(Date.from(readFrom))
      .withStreamName(stream.getStreamName)
      .withShardId(shard.getShardId)
  }
}

/**
  * A source for kinesis records
  * For the stream we maintain a map of current iterator => Future[GetRecordsResponse]
  * and we poll the map every 100ms to send more requests for every completed future
  */
private[timeout] class KinesisSource(
  streamName: String,
  since: ZonedDateTime
)(
  implicit
  val e: ExecutionContext,
  clock: Clock
) extends GraphStage[SourceShape[ByteBuffer]] {

  import KinesisSource._
  val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape = SourceShape[ByteBuffer](outlet)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) with StageLogging {

    // stores futures from kinesis get records requests
    val buffer = mutable.Map.empty[ShardIterator, JavaFuture[GetRecordsResult]]

    Future {
      val stream = kinesis.describeStream(streamName).getStreamDescription
      shardIteratorRequests(since, stream).toStream.par
        .map { shardReq =>
          val i = kinesis.getShardIterator(shardReq).getShardIterator
          val request = new GetRecordsRequest().withShardIterator(i)
          ShardIterator(i, shardReq) -> kinesis.getRecordsAsync(request)
        }.toMap.seq
    }.onComplete(getAsyncCallback[Try[Map[ShardIterator, JavaFuture[GetRecordsResult]]]] { iterators =>
      val unsafeIterators = iterators.get // trigger an exception if we could not bootstrap
      unsafeIterators.foreach(buffer += _)
    }.invoke(_))

    setHandler(outlet, new OutHandler {
      override def onPull() =
        tryToEmitRecords()
    })

    override def onTimer(timerKey: Any) =
      tryToEmitRecords()

    override def beforePreStart() =
      schedulePeriodically("kinesis", 100.millis)

    def emitRecords(currentIterator: ShardIterator, result: GetRecordsResult): Unit = {
      val newIterator = nextIterator(currentIterator, result)
      val nextRequest = new GetRecordsRequest().withShardIterator(newIterator.iterator)
      val newFuture = newIterator -> kinesis.getRecordsAsync(nextRequest)
      emitMultiple[ByteBuffer](outlet, result.getRecords.asScala.map(_.getData).toList, { () =>
        buffer += newFuture
      })
    }

    def reissueIterator(iterator: ShardIterator): Unit = {
      Future {
        kinesis.getShardIterator(iterator.reissue)
      }.map { result =>
        val newIt = iterator.copy(iterator = result.getShardIterator)
        val req = new GetRecordsRequest().withShardIterator(newIt.iterator)
        newIt -> kinesis.getRecordsAsync(req)
      }.onComplete(getAsyncCallback[Try[(ShardIterator, JavaFuture[GetRecordsResult])]] { r =>
        buffer += r.get // again, forcing akka to catch exceptions
      }.invoke(_))
    }

    def retryRequest(iterator: ShardIterator): Unit = {
      val request = new GetRecordsRequest().withShardIterator(iterator.iterator)
      buffer += (iterator -> kinesis.getRecordsAsync(request))
    }

    def tryToEmitRecords() = {
      buffer.filter(_._2.isDone).foreach { case (iterator, future) =>
        buffer.remove(iterator)

        Try(future.get) match {
          case Success(recordsResult) => emitRecords(iterator, recordsResult)
          case Failure(ExpiredIteratorException) => reissueIterator(iterator)
          case Failure(error) =>
            log.debug(error.getMessage)
            retryRequest(iterator)
        }
      }
    }
  }
}
