package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

/**
 * Created by denismo on 18/09/15.
 */
class SizeFlushLimiter(batchSize: Int) extends FlushLimiter {
  var batchCount: Int = 0
  override def isFlushRequired: Boolean = {
    batchCount > batchSize
  }
  override def flushed(writeTime: Long) = {
    batchCount = 0
  }

  override def onRecord(values: Array[String]) = {
    batchCount += 1
  }
}
