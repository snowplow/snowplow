/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq(
    organization  := "com.snowplowanalytics",
    scalaVersion  := "2.11.11",
    scalacOptions := compilerOptions,
    javacOptions  := javaCompilerOptions,
    parallelExecution in Test := false, // Parallel tests cause havoc with Spark
    resolvers     ++= Dependencies.resolutionRepos
  )

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  // Makes our SBT app settings available from within the ETL
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.enrich.spark.generated
        |object ProjectSettings {
        |  val version = "%s"
        |  val name = "%s"
        |  val organization = "%s"
        |  val scalaVersion = "%s"
        |}
        |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )

  lazy val buildSettings = basicSettings ++ scalifySettings

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    // Slightly cleaner jar name
    assemblyJarName in assembly := { name.value + "-" + version.value + ".jar" },
    assemblyMergeStrategy in assembly := {
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.endsWith(".html") => MergeStrategy.discard
      case x if x.endsWith("package-info.class") => MergeStrategy.first
      case PathList("com", "google", "common", tail@_*) => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", tail@_*) => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
