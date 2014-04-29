/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
    organization          :=  "com.snowplowanalytics",
    version               :=  "0.4.0",
    description           :=  "Common functionality for enriching raw Snowplow events",
    scalaVersion          :=  "2.10.1",
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8",
                                  "-unchecked", "-feature"),
    scalacOptions in Test :=  Seq("-Yrangepos"),
    resolvers             ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the ETL
  lazy val scalifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization, scalaVersion) map { (d, v, n, o, sv) =>
    val file = d / "settings.scala"
    IO.write(file, """package com.snowplowanalytics.snowplow.enrich.common.generated
      |object ProjectSettings {
      |  val version = "%s"
      |  val name = "%s"
      |  val organization = "%s"
      |  val scalaVersion = "%s"
      |}
      |""".stripMargin.format(v, n, o, sv))
    Seq(file)
  })

  // For MaxMind support in the test suite
  import Dependencies._
  lazy val maxmindSettings = Seq(

    // Download the GeoLite City and add it into our jar
    resourceGenerators in Test <+= (resourceManaged in Test) map { out =>
      val gzRemote = new URL(Urls.maxmindData)
      val datLocal = out / "maxmind" / "GeoLiteCity.dat"
      
      // Only fetch if we don't already have it (because MaxMind 403s if you download GeoIP.dat.gz too frequently)
      if (!datLocal.exists()) {
        // TODO: replace this with simply IO.gunzipURL(gzRemote, out / "maxmind") when https://github.com/harrah/xsbt/issues/529 implemented
        val gzLocal = out / "GeoLiteCity.dat.gz"        
        IO.download(gzRemote, gzLocal)
        IO.createDirectory(out / "maxmind")
        IO.gunzip(gzLocal, datLocal)
        IO.delete(gzLocal)
        // gunzipURL(gzRemote, out / "maxmind")
      }
      datLocal.get
    }
  )

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

  lazy val buildSettings = basicSettings ++ scalifySettings ++ maxmindSettings ++ publishSettings
}
