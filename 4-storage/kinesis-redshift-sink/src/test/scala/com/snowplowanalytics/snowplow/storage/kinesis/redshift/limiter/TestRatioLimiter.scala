package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

import java.util.Properties

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentialsProvider}
import com.amazonaws.internal.StaticCredentialsProvider
import com.digdeep.util.aws.EmptyAWSCredentialsProvider
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler.{JettyConfigHandler, DripfeedConfig}
import org.scalatest._
import scaldi.Module
import scaldi.{Injector, Injectable}
import Injectable._

trait TestTimeMeasurer extends TimeMeasurer {
  var setTime: Long = 0
  override def getCurrentTime: Long = setTime
}

class RatioFlushLimiterUnderTest(ratio: String, defaultCollectionTime: Long, maxCollectionTime: Long = 100000)(implicit injector: Injector)
  extends RatioFlushLimiter("test", ratio, defaultCollectionTime, maxCollectionTime) with TestTimeMeasurer {}

class TestRatioLimiter extends FunSuite {
  implicit val module: Module = new Module {
    bind [AWSCredentialsProvider] to new EmptyAWSCredentialsProvider()
    bind [Properties] to new Properties()
    bind [DripfeedConfig] to JettyConfigHandler
  }
  test("Ratio limiter returns false by default") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 60000)
    assert(!limiter.isFlushRequired)
  }
  test("Ratio limiter returns true after minWriteTime") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
  }
  test("Ratio limiter returns true after second write") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
    limiter.flushed(0, limiter.setTime, 0)
    limiter.setTime += 6005
    assert(limiter.isFlushRequired)
  }
  test("Ratio limiter returns false when after first write not enough time has passed") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
    val start = limiter.setTime
    limiter.setTime += 800
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 700
    assert(!limiter.isFlushRequired)
    limiter.setTime += 105
    assert(!limiter.isFlushRequired)
    limiter.setTime += 8000
    assert(limiter.isFlushRequired)
  }
  test("No progressive increase after delayed write") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
    val start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(limiter.isFlushRequired)
  }
  test("No progressive increase after delayed write for second time") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
    var start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(limiter.isFlushRequired)
    start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(limiter.isFlushRequired)
  }
  test("Progressive increase after 3 delayed writes") {
    val limiter = new RatioFlushLimiterUnderTest("1/10", 6000)
    limiter.setTime = 6005
    assert(limiter.isFlushRequired)
    var start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(limiter.isFlushRequired)
    start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(limiter.isFlushRequired)
    start = limiter.setTime
    limiter.setTime += 2000
    limiter.flushed(start, limiter.setTime, 0)
    assert(!limiter.isFlushRequired)
    limiter.setTime += 4000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 1000
    assert(!limiter.isFlushRequired)
    limiter.setTime += 3000
    assert(limiter.isFlushRequired)
  }
}