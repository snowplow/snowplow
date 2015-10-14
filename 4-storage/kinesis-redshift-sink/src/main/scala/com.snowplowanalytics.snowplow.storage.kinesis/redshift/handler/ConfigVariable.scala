package com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler

/**
 * Created by denismo on 14/10/15.
 */
trait ConfigVariable {
  type ChangeListener = (String) => Unit
  var notifiers:scala.collection.mutable.MutableList[ChangeListener]
  def get: String
  def set(newValue: String)
  def name: String
}
