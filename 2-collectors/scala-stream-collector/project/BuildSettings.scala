/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the
 * Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.  See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 */
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization          :=  "Snowplow Analytics Ltd",
    version               :=  "0.0.1",
    description           :=  "TODO",
    scalaVersion          :=  "2.10.1",
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8",
                                  "-unchecked", "-feature"),
    scalacOptions in Test :=  Seq("-Yrangepos"),
    maxErrors             := 5,
    // http://www.scala-sbt.org/0.13.0/docs/Detailed-Topics/Forking.html
    fork in run           := true,
    resolvers             ++= Dependencies.resolutionRepos
  )

  // TODO: Where should these go?
  val production: Boolean = true
  val cookieExpirationMs: Long = 365*24*60*60*1000L // 1 year.

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+=
      (sourceManaged in Compile, version, name, organization) map
      { (d, v, n, o) =>
    val file = d / "settings.scala"
    IO.write(file, s"""package com.snowplowanalytics.scalacollector.generated
      |object Settings {
      |  val organization = "$o"
      |  val version = "$v"
      |  val name = "$n"
      |  val production: Boolean = $production
      |  val cookieExpirationMs: Long = ${cookieExpirationMs}L
      |}
      |""".stripMargin)
    Seq(file)
  })

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    // Slightly cleaner jar name
    jarName in assembly := {
      name.value + "-" + version.value + ".jar"
    }
  )

  import com.github.bigtoast.sbtthrift.ThriftPlugin
  // TODO: https://github.com/bigtoast/sbt-thrift defined
  // a thriftSourceDir that I can't figure out how to set to
  // `../thrift-raw-schema`, so I've symlinked the schema for now.

  lazy val buildSettings = basicSettings ++ scalifySettings ++
    sbtAssemblySettings ++ ThriftPlugin.thriftSettings
}
