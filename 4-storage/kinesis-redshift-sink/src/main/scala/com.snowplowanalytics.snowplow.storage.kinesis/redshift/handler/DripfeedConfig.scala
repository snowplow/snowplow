package com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler

/**
 * Created by denismo on 14/10/15.
 */
trait DripfeedConfig {
  def getVariable(name: String) : Option[ConfigVariable]
}
