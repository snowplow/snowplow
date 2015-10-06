package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

import java.util.{Date, Properties}
import java.util.logging.{Level, Logger}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model.{StandardUnit, MetricDatum, PutMetricDataRequest}
import com.digdeep.util.aws.EmptyAWSCredentialsProvider
import org.apache.commons.lang3.math.Fraction
import scaldi.{Injector, Injectable}
import Injectable._

/**
 * Created by denismo on 18/09/15.
 */
object RatioFlushLimiter {
  var stats = List[(Long, Long, Long)]()
  var totalRecords: Long = 0
  var totalFlushedRecords: Long = 0
}

trait TimeMeasurer  {
  def getCurrentTime: Long = System.currentTimeMillis()
}

class RatioFlushLimiter(flushRatio: String, defaultCollectionTime: Long, maxCollectionTime: Long)(implicit injector: Injector) extends FlushLimiter with TimeMeasurer {

  private val log = Logger.getLogger(classOf[RatioFlushLimiter].getName)

  val credentials = inject[AWSCredentialsProvider]

  def getCloudWatchClient(credentials: AWSCredentialsProvider) = {
    if (credentials.isInstanceOf[EmptyAWSCredentialsProvider]) {
      null
    } else {
      val client = new AmazonCloudWatchClient(credentials)
      // TODO Configurable region? Or use the same as Kinesis
      client.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_2))
      client
    }
  }

  val cloudWatch = getCloudWatchClient(credentials)
  val numerator = Fraction.getFraction(flushRatio).getNumerator
  val denominator = Fraction.getFraction(flushRatio).getDenominator
  var lastFlushTime: Long = getCurrentTime
  var collectionTime: Long = 0
  var adjustmentTry: Int = 0
  override def isFlushRequired: Boolean = {
    if (collectionTime == 0) {
      (getCurrentTime - lastFlushTime) > defaultCollectionTime
    } else {
      (getCurrentTime - lastFlushTime) > collectionTime
    }
  }
  override def flushed(writeStart: Long, writeEnd: Long, flushCount: Long) = {
    def adjustWithTry(sign: Int, newCollectionTime: Long): Unit = {
      if (collectionTime == 0) {
        collectionTime = newCollectionTime
      } else if (adjustmentTry*sign > 0) {
        if (adjustmentTry*sign > 3) {
          collectionTime = newCollectionTime
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

    if (currentCollectionTime > maxCollectionTime) {
      adjustmentTry = 0
      if (collectionTime == 0) {
        collectionTime = defaultCollectionTime
      } else {
        collectionTime = Math.min(collectionTime, maxCollectionTime)
      }
    } else {
      val multipliedCollectionTime: Long = currentCollectionTime * numerator
      val multipliedWriteTime: Long = currentWriteTime * denominator
      if (log.isLoggable(Level.FINE)) log.fine("Timings: currentWriteTime=" + currentWriteTime + ", current collection time: " + currentCollectionTime)
      if ((multipliedWriteTime - multipliedCollectionTime)*10 > multipliedCollectionTime ) {
        adjustWithTry(1, currentCollectionTime * 5 / 4)
        // If write time multiplied is more than within 10% of expected ratio then write is too slow - increase the collection time
      } else if (Math.abs(multipliedWriteTime - multipliedCollectionTime)*10 < multipliedCollectionTime) {
        adjustmentTry = 0
        // The write is within 10% - keep the time
      } else {
        adjustWithTry(-1, currentCollectionTime * 4 / 5)
      }
    }

    lastFlushTime = writeStart
    if (log.isLoggable(Level.FINE)) log.fine("New collection time: " + collectionTime + ", try count: " + adjustmentTry)
    if (RatioFlushLimiter.stats.length < 100) {
      RatioFlushLimiter.stats ::= (writeStart, writeEnd, flushCount)
    } else {
      RatioFlushLimiter.stats = List[(Long, Long, Long)]()
    }
    RatioFlushLimiter.totalFlushedRecords += flushCount
    publishToCloudWatch(currentWriteTime, currentCollectionTime, flushCount)
  }
  override def onRecord(values: Array[String]) = {
    RatioFlushLimiter.totalRecords += 1
  }

  // TODO: Extract this out of this class because it is a different concern
  def publishToCloudWatch(writeTime: Long, collectionTime: Long, count: Long): Unit = {
    if (cloudWatch == null) return
    val cloudWatchNamespace = inject[Properties].getProperty("cloudWatchNamespace")
    try {
      val tstamp = new Date()
      val request = new PutMetricDataRequest().withNamespace(cloudWatchNamespace)
        .withMetricData(new MetricDatum().withMetricName("writeTime").withValue(writeTime.toDouble).withUnit(StandardUnit.Milliseconds).withTimestamp(tstamp),
          new MetricDatum().withMetricName("collectionTime").withValue(collectionTime.toDouble).withUnit(StandardUnit.Milliseconds).withTimestamp(tstamp),
          new MetricDatum().withMetricName("writeCount").withValue(count.toDouble).withUnit(StandardUnit.Count).withTimestamp(tstamp))
      cloudWatch.putMetricData(request)
      log.info("Published metrics to CloudWatch at " + cloudWatchNamespace)
    }
    catch {
      case e:Throwable =>
        e.printStackTrace()
        log.log(Level.SEVERE, "Exception publishing to CloudWatch", e)
    }
  }
}
