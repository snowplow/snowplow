package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import java.io.File

import scala.util.Try

object StaticMetrics {

  def javaVersion: String = classOf[Runtime].getPackage.getImplementationVersion

  def scalaVersion: String = Try {
    val props = new java.util.Properties
    props.load(getClass.getResourceAsStream("/library.properties"))
    props.getProperty("version.number")
  }.getOrElse("")

  def runnableName: String = Try {
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getName
  }.getOrElse("")

}
