package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

import org.scalatest._

class TestRatioLimiter extends FunSuite {
  test("Ratio limiter returns false by default") {
    val limiter = new RatioFlushLimiter(1, 10, 6000)
    assert(!limiter.isFlushRequired)
  }
  test("Ratio limiter returns true after minWriteTime") {
    val limiter = new RatioFlushLimiter(1, 10, 6000)
    Thread.sleep(6005)
    assert(limiter.isFlushRequired)
  }
  test("Ratio limiter returns false when after first write not enough time has passed") {
    val limiter = new RatioFlushLimiter(1, 10, 600)
    Thread.sleep(605)
    assert(limiter.isFlushRequired)
    val start = System.currentTimeMillis()
    Thread.sleep(800)
    limiter.flushed(start, System.currentTimeMillis(), 0)
    assert(!limiter.isFlushRequired)
    Thread.sleep(700)
    assert(!limiter.isFlushRequired)
    Thread.sleep(105)
    assert(!limiter.isFlushRequired)
    Thread.sleep(8000)
    assert(limiter.isFlushRequired)
  }
}