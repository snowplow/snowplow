package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

/**
 * Created by denismo on 18/09/15.
 */
trait FlushLimiter {
  def isFlushRequired: Boolean
  def flushed(writeStart: Long, writeEnd: Long, flushCount: Long)
  def onRecord(values: Array[String])
}
