/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
    organization  := "com.snowplowanalytics",
    version       := "1.7.0",
    description   := "The Snowplow spark Enrichment process, written in Scalding for spark 2.4",
    scalaVersion  := "2.10.4",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8",
                         "-target:jvm-1.7"),
    parallelExecution in Test := false, // Parallel tests cause havoc with spark
    logBuffered   := false,             // For debugging Specs2 tests
    resolvers     ++= Dependencies.resolutionRepos,
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    fork in run   := true,
    //ivyScala      := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    javacOptions  ++= Seq("-source", "1.8", "-target", "1.8")
  )

  // Makes our SBT app settings available from within the ETL
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization, scalaVersion) map { (d, v, n, o, sv) =>
    val file = d / "settings.scala"
    IO.write(file, """package com.snowplowanalytics.snowplow.enrich.spark.generated
      |object ProjectSettings {
      |  val version = "%s"
      |  val name = "%s"
      |  val organization = "%s"
      |  val scalaVersion = "%s"
      |}
      |""".stripMargin.format(v, n, o, sv))
    Seq(file)
  })

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._

  lazy val sbtAssemblySettings = Seq(

    // Simpler jar name
    assemblyJarName in assembly := {
      name.value + "-" + version.value + ".jar"
    },

    // Drop these jars
    assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "junit-4.5.jar", // We shouldn't need JUnit
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "minlog-1.2.jar", // Otherwise causes conflicts with Kyro (which bundles it)
        "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
        "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
        "commons-beanutils-1.7.0.jar",      // "
        "protobuf-java-2.4.1.jar", // Hadoop needs 2.5.0
        "slf4j-api-1.6.4.jar",
        "httpclient-4.1.3.jar",
        "httpcore-4.1.3.jar",
        "xpp3_min-1.1.4c.jar",
        "solr-commons-csv-3.5.0.jar",
        "commons-logging-1.1.3.jar",
        "servlet-api-2.5.jar",
        "jsp-api-2.1.jar",
        "cassandra-driver-core-2.1.3-sources.jar",
        "minlog-1.2.jar",
        "jcl-over-slf4j-1.7.5.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
      (old) => {
        case x if x.endsWith("project.clj") => MergeStrategy.discard // Leiningen build files
        case x if x.startsWith("META-INF") => MergeStrategy.discard // More bumf
        case x if x.endsWith(".html") => MergeStrategy.discard
        case x if x.endsWith("package-info.class") => MergeStrategy.first
        case PathList("com", "google", "common", tail@_*) =>
          MergeStrategy.first
        case PathList("org", "apache", "spark", "unused", tail@_*) =>
          MergeStrategy.first
        case x => old(x)
      }
    }
  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++ sbtAssemblySettings
}
