package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

/**
 * Created by denismo on 18/09/15.
 */
class RatioFlushLimiter(numerator: Int, denominator: Int, minWriteTime: Long) extends FlushLimiter {
  var writeTime: Long = 0
  var lastFlushTime: Long = System.currentTimeMillis()
  override def isFlushRequired: Boolean = {
    if (writeTime == 0) {
      System.currentTimeMillis() - lastFlushTime > minWriteTime
    } else {
      //      val diff = System.currentTimeMillis() - lastFlushTime
      //      println (diff * numerator, denominator * writeTime)
      (System.currentTimeMillis() - lastFlushTime) * numerator > denominator * writeTime
    }
  }
  override def flushed(writeTime: Long) = {
    this.lastFlushTime = System.currentTimeMillis()
    this.writeTime = writeTime
  }
  override def onRecord(values: Array[String]) = {}
}
