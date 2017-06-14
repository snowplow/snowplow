/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

// sbt-assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._

/**
 * Common settings-patterns for Snowplow apps and libraries.
 * To enable any of these you need to explicitly add Settings value to build.sbt
 */
object BuildSettings {

  /**
   * Common set of useful and restrictive compiler flags
   */
  lazy val buildSettings = Seq(
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Ywarn-unused-import",
      "-Ywarn-nullary-unit",
      "-Xfatal-warnings",
      "-Xlint",
      "-Yinline-warnings",
      "-language:higherKinds",
      "-Ypartial-unification",
      "-Xfuture"),

    scalacOptions in (Compile, console) := Seq(
      "-deprecation",
      "-encoding", "UTF-8"
    ),

    addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary)
  )

  // sbt-assembly settings
  lazy val assemblySettings = Seq(
    assemblyJarName in assembly := { name.value + "-" + version.value + ".jar" },

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case PathList("reference.conf", _ @ _*) => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )

  /**
   * Makes package (build) metadata available withing source code
   */
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.rdbloader.generated
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
}
