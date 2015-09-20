package com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter

import org.apache.commons.logging.LogFactory

/**
 * Created by denismo on 18/09/15.
 */
object RatioFlushLimiter {
  var stats = List[(Long, Long)]()
  var totalRecords: Long = 0
  var totalFlushedRecords: Long = 0
}

class RatioFlushLimiter(numerator: Int, denominator: Int, minWriteTime: Long) extends FlushLimiter {
  private val log = LogFactory.getLog(classOf[RatioFlushLimiter])

  var writeTime: Long = 0
  var lastFlushTime: Long = System.currentTimeMillis()
  override def isFlushRequired: Boolean = {
    if (writeTime == 0) {
      System.currentTimeMillis() - lastFlushTime > minWriteTime
    } else {
      (System.currentTimeMillis() - lastFlushTime) * numerator > denominator * writeTime
/*
      val millis: Long = System.currentTimeMillis()
      //      val diff = System.currentTimeMillis() - lastFlushTime
      //      println (diff * numerator, denominator * writeTime)
      val res = (millis - lastFlushTime) * numerator > denominator * writeTime
      if (res) {
        log.info("%s > %s".format(millis - lastFlushTime, writeTime))
      }
      res
*/
    }
  }
  override def flushed(writeStart: Long, writeEnd: Long, flushCount: Long) = {
    this.lastFlushTime = writeStart
    this.writeTime = writeEnd-writeStart
    RatioFlushLimiter.stats ::= (writeStart, writeEnd)
    RatioFlushLimiter.totalFlushedRecords += flushCount
  }
  override def onRecord(values: Array[String]) = {
    RatioFlushLimiter.totalRecords += 1
  }
}
