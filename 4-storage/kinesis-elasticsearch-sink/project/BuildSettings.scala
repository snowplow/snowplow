/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

 // SBT
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization :=  "com.snowplowanalytics",
    version      :=  "0.4.1",
    description  :=  "Kinesis sink for Elasticsearch",
    resolvers    ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization) map { (d, v, n, o) =>
    val file = d / "settings.scala"
    IO.write(file, """package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch.generated
      |object Settings {
      |  val organization = "%s"
      |  val version = "%s"
      |  val name = "%s"
      |}
      |""".stripMargin.format(o, v, n))
    Seq(file)
  })

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  import sbtassembly.AssemblyPlugin.defaultShellScript

  lazy val sbtAssemblySettings = baseAssemblySettings ++ Seq(
    // Executable jarfile
    assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) },
    // Name it as an executable
    assemblyJarName in assembly := { s"${name.value}-${version.value}" },
    // Patch for joda-convert merge issue with elasticsearch 1.7.0
    // See: https://github.com/elastic/elasticsearch/pull/10294
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter {_.data.getName == "joda-convert-1.2.jar"}
    }
  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings
}
