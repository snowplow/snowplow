/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
// =======================================================
// scalafmt: {align.tokens = ["%", "%%"]}
// =======================================================
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
     // For some misc Scalding and Twitter libs
    "Concurrent Maven Repo" at "http://conjars.org/repo",
     // For Twitter's util functions
    "Twitter Maven Repo" at "http://maven.twttr.com/",
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For uaParser utils
    "user-agent-parser repo" at "https://clojars.org/repo/"
  )

  object V {
    // Java
    val commonsCodec     = "1.12"
    val jodaTime         = "2.10.1"
    val useragent        = "1.21"
    val jacksonDatabind  = "2.9.8"
    val uaParser         = "1.4.3"
    val postgresDriver   = "42.2.5"
    val mysqlConnector   = "5.1.39"
    val jaywayJsonpath   = "2.4.0"
    val iabClient        = "0.1.0"
    val yauaa            = "5.8"
    val kryo             = "2.24.0"
    val rhino            = "1.7.10"
    // Scala
    val cats             = "1.6.0"
    val refererParser    = "0.6.0-M2"
    val maxmindIplookups = "0.6.0-M1"
    val circe            = "0.11.1"
    val circeOptics      = "0.11.0"
    val circeJackson     = "0.11.1"
    val igluClient       = "0.6.0-M6"
    val scalaForex       = "0.7.0-M4"
    val scalaWeather     = "0.5.0-M2"
    val scalaj           = "2.3.0"
    val gatlingJsonpath  = "0.6.14"
    val scalaUri         = "1.4.3"
    val scalaLruMap      = "0.3.0-M3"
    // Thrift schemas
    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val schemaSniffer    = "0.0.0"
    // Scala (test only)
    val specs2           = "4.5.1"
    val scalaCheck       = "1.10.0"
    val scaldingArgs     = "0.13.0"
    val mockito          = "1.10.19"
  }

  object Libraries {
    // Java
    val commonsCodec     = "commons-codec"              %  "commons-codec"                 % V.commonsCodec
    val jodaTime         = "joda-time"                  %  "joda-time"                     % V.jodaTime
    val useragent        = "eu.bitwalker"               %  "UserAgentUtils"                % V.useragent
    val jacksonDatabind  = "com.fasterxml.jackson.core" %  "jackson-databind"              % V.jacksonDatabind
    val uaParser         = "com.github.ua-parser"       %  "uap-java"                      % V.uaParser
    val postgresDriver   = "org.postgresql"             %  "postgresql"                    % V.postgresDriver
    val mysqlConnector   = "mysql"                      %  "mysql-connector-java"          % V.mysqlConnector
    val jaywayJsonpath   = "com.jayway.jsonpath"        %  "json-path"                     % V.jaywayJsonpath
    val iabClient        = "com.snowplowanalytics"      %  "iab-spiders-and-robots-client" % V.iabClient
    val yauaa            = "nl.basjes.parse.useragent"  %  "yauaa"                         % V.yauaa
    val kryo             = "com.esotericsoftware.kryo"  %  "kryo"                          % V.kryo
    val rhino            = "org.mozilla"                %  "rhino"                         % V.rhino

    // Scala
    val circeDeps        = List(
      "circe-core",
      "circe-generic",
      "circe-parser",
      "circe-literal"
    ).map("io.circe" %% _ % V.circe)
    val cats             = "org.typelevel"              %% "cats-core"                     % V.cats
    val circeOptics      = "io.circe"                   %% "circe-optics"                  % V.circeOptics
    val circeJackson     = "io.circe"                   %% "circe-jackson29"               % V.circeJackson
    val scalaForex       = "com.snowplowanalytics"      %% "scala-forex"                   % V.scalaForex
    val refererParser    = "com.snowplowanalytics"      %% "scala-referer-parser"          % V.refererParser
    val maxmindIplookups = "com.snowplowanalytics"      %% "scala-maxmind-iplookups"       % V.maxmindIplookups
    val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"             % V.igluClient
    val scalaUri         = "io.lemonlabs"               %% "scala-uri"                     % V.scalaUri
    val scalaWeather     = "com.snowplowanalytics"      %% "scala-weather"                 % V.scalaWeather
    val scalaj           = "org.scalaj"                 %% "scalaj-http"                   % V.scalaj
    val gatlingJsonpath  = "io.gatling"                 %% "jsonpath"                      % V.gatlingJsonpath
    val scalaLruMap      = "com.snowplowanalytics"      %% "scala-lru-map"                 % V.scalaLruMap
    // Thrift schemas
    val snowplowRawEvent = "com.snowplowanalytics"      %  "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"      %  "collector-payload-1"           % V.collectorPayload
    val schemaSniffer    = "com.snowplowanalytics"      %  "schema-sniffer-1"              % V.schemaSniffer
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2-core"                   % V.specs2         % "test"
    val specs2Cats       = "org.specs2"                 %% "specs2-cats"                   % V.specs2         % "test"
    val specs2Scalacheck = "org.specs2"                 %% "specs2-scalacheck"             % V.specs2         % "test"
    val specs2Mock       = "org.specs2"                 %% "specs2-mock"                   % V.specs2         % "test"
  }
}
