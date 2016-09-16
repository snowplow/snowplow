/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.AssemblyPlugin.defaultShellScript
import sbt._
import Keys._
import scala.io.Source._

object BuildSettings {

  // Defines the ES Version to build for
  val ElasticsearchVersion = sys.env("ELASTICSEARCH_VERSION")

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization          :=  "com.snowplowanalytics",
    version               :=  "0.8.0-rc1",
    description           :=  "Kinesis sink for Elasticsearch",
    scalaVersion          :=  "2.10.1",
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8",
                                  "-feature", "-target:jvm-1.7"),
    scalacOptions in Test :=  Seq("-Yrangepos"),
    resolvers             ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization) map { (d, v, n, o) =>
    val settingsFile = d / "settings.scala"
    IO.write(settingsFile, """package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch.generated
      |object Settings {
      |  val organization = "%s"
      |  val version = "%s"
      |  val name = "%s"
      |}
      |""".stripMargin.format(o, v, n)
    )

    // Dynamically load ElasticsearchClients
    val genDir = new java.io.File("").getAbsolutePath + "/src-compat/main/scala/com.snowplowanalytics.snowplow.storage.kinesis/elasticsearch/generated/"

    val esHttpClientFile = d / "ElasticsearchSenderHTTP.scala"
    val esHttpClientLines = (if (ElasticsearchVersion.equals("1x")) {
      fromFile(genDir + "ElasticsearchSenderHTTP_1x.scala")
    } else {
      fromFile(genDir + "ElasticsearchSenderHTTP_2x.scala")
    })
    IO.write(esHttpClientFile, esHttpClientLines.mkString)

    val esTransportClientFile = d / "ElasticsearchSenderTransport.scala"
    val esTransportClientLines = (if (ElasticsearchVersion.equals("1x")) {
      fromFile(genDir + "ElasticsearchSenderTransport_1x.scala")
    } else {
      fromFile(genDir + "ElasticsearchSenderTransport_2x.scala")
    })
    IO.write(esTransportClientFile, esTransportClientLines.mkString)

    Seq(
      settingsFile,
      esHttpClientFile,
      esTransportClientFile
    )
  })

  // Assembly settings
  lazy val sbtAssemblySettings: Seq[Setting[_]] = Seq(

    // Executable jarfile
    assemblyOption in assembly ~= { 
      _.copy(prependShellScript = Some(defaultShellScript)) 
    },

    // Name it as an executable
    assemblyJarName in assembly := { 
      s"${name.value}-${version.value}-${ElasticsearchVersion}" 
    },

    // Merge duplicate class in JodaTime and Elasticsearch 2.4
    mergeStrategy in assembly := {
      case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
      case x => (mergeStrategy in assembly).value(x)
    }
  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings
}
