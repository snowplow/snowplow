package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

/**
 * Created by denismo on 18/09/15.
 */
class OrLimiter(one:FlushLimiter, two:FlushLimiter) extends FlushLimiter {
  override def isFlushRequired: Boolean = one.isFlushRequired || two.isFlushRequired

  override def flushed(writeTime: Long): Unit = {
    one.flushed(writeTime)
    two.flushed(writeTime)
  }

  override def onRecord(values: Array[String]): Unit = {
    one.onRecord(values)
    two.onRecord(values)
  }
}
