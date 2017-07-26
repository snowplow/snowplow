/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-common-enrich",
    version     := "0.25.0",
    description := "Common functionality for enriching raw Snowplow events"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.httpClient,
      Dependencies.Libraries.yodaTime,
      Dependencies.Libraries.yodaConvert,
      Dependencies.Libraries.commonsLang,
      Dependencies.Libraries.commonsIo,
      Dependencies.Libraries.commonsCodec,
      Dependencies.Libraries.useragent,
      Dependencies.Libraries.jacksonDatabind,
      Dependencies.Libraries.jsonValidator,
      Dependencies.Libraries.mavenArtifact,
      Dependencies.Libraries.uaParser,
      Dependencies.Libraries.postgresDriver,
      Dependencies.Libraries.mysqlConnector,
      // Scala
      Dependencies.Libraries.scalaz7,
      Dependencies.Libraries.snowplowRawEvent,
      Dependencies.Libraries.collectorPayload,
      Dependencies.Libraries.schemaSniffer,
      Dependencies.Libraries.refererParser,
      Dependencies.Libraries.maxmindIplookups,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.json4sScalaz,
      Dependencies.Libraries.igluClient,
      Dependencies.Libraries.scalaUri,
      Dependencies.Libraries.scalaForex,
      Dependencies.Libraries.scalaWeather,
      Dependencies.Libraries.scalaj,
      Dependencies.Libraries.gatlingJsonpath,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.scalazSpecs2,
      Dependencies.Libraries.scalaCheck,
      Dependencies.Libraries.scaldingArgs,
      Dependencies.Libraries.mockito
    )
  )
