package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.specs2.Specification

class RemoteAdaptersSpec extends Specification {
  def is = sequential ^ s2"""
                This is a specification to test some RemoteAdapters functionality.
                Should be able to create RemoteAdapters from a string config     $e1
                Should be able to create RemoteAdapters from a resource config   $e2
                Should be able to create RemoteAdapters from a file config       $e3
                Should error out on a bad config                                 $e4
  """

  val vendor            = "com.blarg"
  val version           = "v1"
  val url               = "http://remoteTestSystem:8995/customRemoteEnricher"
  val timeout           = "4s"
  val ourTestConfigFile = "RemoteAdapters.conf"

  def e1 = {

    val testConfig =
      s"""
         |akka{actor{provider:local}}
         |remoteAdapters:[ {vendor:\"$vendor\", version:\"$version\", url:\"$url\", timeout:\"$timeout\"} ]
        """.stripMargin

    val actual = RemoteAdapters.createFromConfigString(testConfig)
    commonValidation(actual)
  }

  def e2 = {

    val testConfig = ConfigFactory.parseResources(this.getClass, ourTestConfigFile)

    val actual = RemoteAdapters.createFromConfig(testConfig)
    commonValidation(actual)
  }

  def e3 = {
    val classPath = this.getClass.getPackage.getName.replace(".", "/")

    val actual = RemoteAdapters.createFromConfigFile(s"src/test/resources/$classPath/$ourTestConfigFile")
    commonValidation(actual)
  }

  def e4 =
    RemoteAdapters.createFromConfigString("bad{config") must throwA[ConfigException]

  def commonValidation(actual: Map[(String, String), RemoteAdapter]) = {
    val theAdapter = actual.get((vendor, version))

    (
      theAdapter must beSome[RemoteAdapter]
        and (theAdapter.get.timeout.toMillis must beEqualTo(4000))
        and (theAdapter.get.remoteUrl must beEqualTo(url))
    )
  }
}
