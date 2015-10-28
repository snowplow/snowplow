package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, Properties}
import java.util.logging.{Level, Logger}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.{RegionUtils, Regions, Region}
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model.{Dimension, StandardUnit, MetricDatum, PutMetricDataRequest}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.digdeep.util.aws.EmptyAWSCredentialsProvider
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler.{DripfeedConfig, JettyConfigHandler, BasicConfigVariable}
import org.apache.commons.lang3.math.Fraction
import scaldi.{Injector, Injectable}
import scaldi.Injectable._

/**
 * Created by denismo on 18/09/15.
 */
object RatioFlushLimiter {
  var stats = List[(Long, Long, Long)]()
  val totalRecords = new AtomicLong()
  val totalKinesisRecords = new AtomicLong()
  val totalFlushedRecords = new AtomicLong()
}

trait TimeMeasurer  {
  def getCurrentTime: Long = System.currentTimeMillis()
}

class RatioFlushLimiter(table: String, flushRatio: String, defaultCollectionTime: Long, var maxCollectionTime: Long)(implicit injector: Injector) extends FlushLimiter with TimeMeasurer {

  private val log = Logger.getLogger(classOf[RatioFlushLimiter].getName)

  val credentials = inject[AWSCredentialsProvider]

  def getCloudWatchClient(credentials: AWSCredentialsProvider) = {
    if (credentials.isInstanceOf[EmptyAWSCredentialsProvider]) {
      None
    } else {
      val client = new AmazonCloudWatchClient(credentials)
      client.setRegion(RegionUtils.getRegion(inject[Properties].getProperty(KinesisConnectorConfiguration.PROP_REGION_NAME)))
      Some(client)
    }
  }
  {
    inject[DripfeedConfig].getVariable("maxCollectionTime").foreach(v => {
      v.asInstanceOf[BasicConfigVariable].notifiers += (newValue => maxCollectionTime = newValue.toLong)
    })
    inject[DripfeedConfig].getVariable("collectionTime").foreach(v => {
      v.asInstanceOf[BasicConfigVariable].notifiers += (newValue => collectionTime = newValue.toLong)
    })
    inject[DripfeedConfig].getVariable("flushRatio").foreach(v => {
      v.asInstanceOf[BasicConfigVariable].notifiers += (newValue => {
        numerator = Fraction.getFraction(flushRatio).getNumerator
        denominator = Fraction.getFraction(flushRatio).getDenominator
      })
    })
  }

  val cloudWatch = getCloudWatchClient(credentials)
  var numerator = Fraction.getFraction(flushRatio).getNumerator
  var denominator = Fraction.getFraction(flushRatio).getDenominator
  var lastFlushTime: Long = getCurrentTime
  var collectionTime: Long = defaultCollectionTime
  var adjustmentTry: Int = 0
  override def isFlushRequired: Boolean = {
    (getCurrentTime - lastFlushTime) > collectionTime
  }
  override def flushed(writeStart: Long, writeEnd: Long, flushCount: Long) = {
    def adjustWithTry(sign: Int, newCollectionTime: Long): Unit = {
      if (adjustmentTry != 0 && sign != Math.signum(adjustmentTry)) {
        // Sign change - means current value is OK with regards to initial hypothesis, so reset
        adjustmentTry = 0
      } else if (adjustmentTry != 0) {
        if (Math.abs(adjustmentTry) >= 2) {
          collectionTime = Math.min(newCollectionTime, maxCollectionTime)
          adjustmentTry = 0
        } else {
          adjustmentTry += sign
        }
      } else {
        adjustmentTry += sign
      }
    }

    val currentWriteTime = writeEnd - writeStart
    val currentCollectionTime: Long = writeStart - lastFlushTime

    // The wait went over - probably because there were no events
    if (currentCollectionTime > maxCollectionTime) {
      adjustmentTry = 0
    } else {
      val multipliedCollectionTime: Long = collectionTime * numerator
      val multipliedWriteTime: Long = currentWriteTime * denominator
      if (log.isLoggable(Level.FINE)) log.fine(s"Timings: currentWriteTime=$currentWriteTime, collection time=$collectionTime, time since last flush=$currentCollectionTime")

      if ((multipliedWriteTime - multipliedCollectionTime)*10 > multipliedCollectionTime ) {
        if (log.isLoggable(Level.FINE)) log.fine("Increasing to " + collectionTime * 5 / 4)
        adjustWithTry(1, collectionTime * 5 / 4)
        // If write time multiplied is more than within 10% of expected ratio then write is too slow - increase the collection time
      } else if (Math.abs(multipliedWriteTime - multipliedCollectionTime)*10 < multipliedCollectionTime) {
        adjustmentTry = 0
        // The write is within 10% - keep the time
      } else {
        if (log.isLoggable(Level.FINE)) log.fine("Decreasing to " + collectionTime * 4 / 5)
        adjustWithTry(-1, collectionTime * 4 / 5)
      }
    }

    lastFlushTime = writeStart
    if (log.isLoggable(Level.FINE)) log.fine("New collection time: " + collectionTime + ", try count: " + adjustmentTry)
    if (RatioFlushLimiter.stats.length < 100) {
      RatioFlushLimiter.stats ::= (writeStart, writeEnd, flushCount)
    } else {
      RatioFlushLimiter.stats = List[(Long, Long, Long)]((writeStart, writeEnd, flushCount))
    }
    RatioFlushLimiter.totalFlushedRecords.addAndGet(flushCount)
    publishToCloudWatch(currentWriteTime, collectionTime, currentCollectionTime, flushCount)
  }
  override def onRecord(values: Array[String]) = {
    RatioFlushLimiter.totalRecords.incrementAndGet()
  }

  // TODO: Extract this out of this class because it is a different concern
  def publishToCloudWatch(writeTime: Long, collectionTime: Long, timeSinceLastFlush: Long, count: Long): Unit = {
    cloudWatch.foreach { client =>
      val cloudWatchNamespace = inject[Properties].getProperty("cloudWatchNamespace")
      try {
        val tstamp = new Date()
        val request = new PutMetricDataRequest().withNamespace(cloudWatchNamespace)
          .withMetricData(
            new MetricDatum().withMetricName("writeTime").withValue(writeTime.toDouble).withUnit(StandardUnit.Milliseconds).withTimestamp(tstamp)
              .withDimensions(new Dimension().withName("Table").withValue(table)),
            new MetricDatum().withMetricName("collectionTime").withValue(collectionTime.toDouble).withUnit(StandardUnit.Milliseconds).withTimestamp(tstamp)
              .withDimensions(new Dimension().withName("Table").withValue(table)),
            new MetricDatum().withMetricName("timeSinceLastFlush").withValue(timeSinceLastFlush.toDouble).withUnit(StandardUnit.Milliseconds).withTimestamp(tstamp)
              .withDimensions(new Dimension().withName("Table").withValue(table)),
            new MetricDatum().withMetricName("writeCount").withValue(count.toDouble).withUnit(StandardUnit.Count).withTimestamp(tstamp)
              .withDimensions(new Dimension().withName("Table").withValue(table)))
        client.putMetricData(request)
        log.info("Published metrics to CloudWatch at " + cloudWatchNamespace)
      }
      catch {
        case e:Throwable =>
          e.printStackTrace()
          log.log(Level.SEVERE, "Exception publishing to CloudWatch", e)
      }
    }
  }
}
