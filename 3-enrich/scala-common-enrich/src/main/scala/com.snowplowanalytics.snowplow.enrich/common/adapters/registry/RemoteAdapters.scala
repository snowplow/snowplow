package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object RemoteAdapters {

  lazy val log = LoggerFactory.getLogger(getClass)

  var Adapters = Map.empty[(String, String), RemoteAdapter]

  def createFromConfigFile(configFilename: String) =
    if (configFilename != null) {
      try {
        val configFile = new File(configFilename)
        if (configFile.exists()) {
          createFromConfig(ConfigFactory.parseFile(configFile))
        } else {
          log.error(s"RemoteAdapters config file '$configFilename' was not found!")
          Map.empty[(String, String), RemoteAdapter]
        }
      } catch {
        case e: Exception =>
          log.error(s"RemoteAdapters initialization failed!", e)
          Map.empty[(String, String), RemoteAdapter]
      }
    } else {
      Map.empty[(String, String), RemoteAdapter]
    }

  def createFromConfigString(config: String) =
    createFromConfig(ConfigFactory.parseString(config))

  def createFromConfig(userConfig: Config) = {
    val config = ConfigFactory.load(userConfig)

    config
      .getConfigList("remoteAdapters")
      .asScala
      .toList
      .map { adapterConf =>
        val durationInMillis = adapterConf.getDuration("timeout", TimeUnit.MILLISECONDS)
        val adapter =
          new RemoteAdapter(adapterConf.getString("url"), new FiniteDuration(durationInMillis, TimeUnit.MILLISECONDS))

        (adapterConf.getString("vendor"), adapterConf.getString("version")) -> adapter
      }
      .toMap
  }

}
