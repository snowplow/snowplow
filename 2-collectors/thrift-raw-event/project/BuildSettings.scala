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

// Thrift
import com.github.bigtoast.sbtthrift.ThriftPlugin

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization          :=  "com.snowplowanalytics",
    version               :=  "0.1.0",
    description           :=  "Generates POJOs for the Snowplow Raw Event as defined in Thrift IDL",
    scalaVersion          :=  "2.10.1",
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8",
                                  "-unchecked", "-feature"),
    scalacOptions in Test :=  Seq("-Yrangepos"),
    maxErrors             := 5,
    // http://www.scala-sbt.org/0.13.0/docs/Detailed-Topics/Forking.html
    fork in run           := true,
    resolvers             ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+=
      (sourceManaged in Compile, version, name, organization) map
      { (d, v, n, o) =>
    val file = d / "settings.scala"
    IO.write(file, s"""package com.snowplowanalytics.snowplow.collectors.thrift
      |object Settings {
      |  val organization = "$o"
      |  val version = "$v"
      |  val name = "$n"
      |}
      |""".stripMargin)
    Seq(file)
  })

  // Publish settings
  // TODO: update with ivy credentials etc when we start using Nexus
  lazy val publishSettings = Seq[Setting[_]](
   
    crossPaths := false,
    publishTo <<= version { version =>
      val keyFile = (Path.userHome / ".ssh" / "admin_keplar.osk")
      val basePath = "/var/www/maven.snplow.com/prod/public/%s".format {
        if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases/"
      }
      Some(Resolver.sftp("SnowPlow Analytics Maven repository", "prodbox", 8686, basePath) as ("admin", keyFile))
    }
  )

  lazy val buildSettings = basicSettings ++ scalifySettings ++
    ThriftPlugin.thriftSettings ++ publishSettings
}
