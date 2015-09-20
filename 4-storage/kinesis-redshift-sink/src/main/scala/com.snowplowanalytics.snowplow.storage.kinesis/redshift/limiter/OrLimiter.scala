package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

/**
 * Created by denismo on 18/09/15.
 */
class OrLimiter(one:FlushLimiter, two:FlushLimiter) extends FlushLimiter {
  override def isFlushRequired: Boolean = one.isFlushRequired || two.isFlushRequired

  override def flushed(writeStart: Long, writeEnd: Long, flushCount: Long): Unit = {
    one.flushed(writeStart, writeEnd, flushCount)
    two.flushed(writeStart, writeEnd, flushCount)
  }

  override def onRecord(values: Array[String]): Unit = {
    one.onRecord(values)
    two.onRecord(values)
  }
}
