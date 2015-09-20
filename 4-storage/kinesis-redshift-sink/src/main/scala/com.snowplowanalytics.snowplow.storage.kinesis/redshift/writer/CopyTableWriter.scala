package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

/**
 * Created by denismo on 18/09/15.
 */
trait CopyTableWriter {
  def flush(): Unit
  def close()
  def write(values: Array[String])
  def write(value: String)
  def requiresJsonParsing: Boolean
}
