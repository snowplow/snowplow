package com.snowplowanalytics.snowplow.storage.kinesis.redshift.handler

/**
 * Created by denismo on 14/10/15.
 */
class BasicConfigVariable(_name: String, var defaultValue: String) extends ConfigVariable {
  override var notifiers = scala.collection.mutable.MutableList[(String) => Unit]()

  override def get: String = {
    defaultValue
  }

  override def set(newValue: String): Unit = {
    defaultValue = newValue
    notifiers.foreach(callback => callback(newValue))
  }

  override def name: String = _name
}
