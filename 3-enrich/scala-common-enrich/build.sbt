/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
// =======================================================
// scalafmt: {align.tokens = [":="]}
// =======================================================
lazy val root = project
  .in(file("."))
  .settings(
    name := "snowplow-common-enrich",
    version := "1.1.0",
    description := "Common functionality for enriching raw Snowplow events"
  )
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(parallelExecution in Test := false)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.jodaTime,
      Dependencies.Libraries.commonsCodec,
      Dependencies.Libraries.useragent,
      Dependencies.Libraries.jacksonDatabind,
      Dependencies.Libraries.uaParser,
      Dependencies.Libraries.postgresDriver,
      Dependencies.Libraries.mysqlConnector,
      Dependencies.Libraries.jaywayJsonpath,
      Dependencies.Libraries.iabClient,
      Dependencies.Libraries.yauaa,
      Dependencies.Libraries.kryo,
      Dependencies.Libraries.rhino,
      Dependencies.Libraries.guava,
      // Scala
      Dependencies.Libraries.cats,
      Dependencies.Libraries.circeOptics,
      Dependencies.Libraries.circeJackson,
      Dependencies.Libraries.refererParser,
      Dependencies.Libraries.maxmindIplookups,
      Dependencies.Libraries.scalaUri,
      Dependencies.Libraries.scalaForex,
      Dependencies.Libraries.scalaWeather,
      Dependencies.Libraries.scalaj,
      Dependencies.Libraries.gatlingJsonpath,
      Dependencies.Libraries.scalaLruMap,
      Dependencies.Libraries.badRows,
      // Thrift schemas
      Dependencies.Libraries.snowplowRawEvent,
      Dependencies.Libraries.collectorPayload,
      Dependencies.Libraries.schemaSniffer,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2Cats,
      Dependencies.Libraries.specs2Scalacheck,
      Dependencies.Libraries.specs2Mock
    ) ++ Dependencies.Libraries.circeDeps
  )
