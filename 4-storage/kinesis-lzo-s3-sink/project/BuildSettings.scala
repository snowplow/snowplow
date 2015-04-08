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
    organization          :=  "com.snowplowanalytics",
    version               :=  "0.2.0",
    description           :=  "Kinesis LZO sink for S3",
    scalaVersion          :=  "2.10.1",
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8",
                                  "-feature", "-target:jvm-1.7"),
    scalacOptions in Test :=  Seq("-Yrangepos"),
    resolvers             ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization) map { (d, v, n, o) =>
    val file = d / "settings.scala"
    IO.write(file, """package com.snowplowanalytics.snowplow.storage.kinesis.s3.generated
      |object Settings {
      |  val organization = "%s"
      |  val version = "%s"
      |  val name = "%s"
      |}
      |""".stripMargin.format(o, v, n))
    Seq(file)
  })

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    // Executable jarfile
    assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) },
    // Name it as an executable
    jarName in assembly := { s"${name.value}-${version.value}" },

    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "junit-4.8.2.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "jsp-api-2.1-6.1.14.jar",
        "servlet-api-2.5-6.1.14.jar",
        "commons-beanutils-1.7.0.jar",
        "hadoop-lzo-0.4.19.jar",
        "stax-api-1.0.1.jar",
        "commons-collections-3.2.1.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    mergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
      case PathList("org", "objectweb", "asm", xs @ _*)  => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case "application.conf"                            => MergeStrategy.concat
      case x =>
        val oldStrategy = (mergeStrategy in assembly).value
        oldStrategy(x)
    }

  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings
}
