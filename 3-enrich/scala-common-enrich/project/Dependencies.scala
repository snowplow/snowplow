/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    // Required for our json4s snapshot
    "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
     // For some misc Scalding and Twitter libs
    "Concurrent Maven Repo" at "http://conjars.org/repo",
     // For Twitter's util functions
    "Twitter Maven Repo" at "http://maven.twttr.com/",
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For user-agent-utils
    "user-agent-utils repo" at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/",
    // For uaParser utils
    "user-agent-parser repo" at "https://clojars.org/repo/"
  )

  object V {
    // Java
    val http             = "4.3.3"
    val commonsLang      = "3.1"
    val commonsIo        = "2.4"
    val yodaTime         = "2.1"
    val yodaConvert      = "1.2"
    val useragent        = "1.14"
    val jacksonDatabind  = "2.2.3"
    val jsonValidator    = "2.2.3"
    val mavenArtifact    = "3.2.2"
    val uaParser         = "1.3.0"
    // Scala
    val scalaz7          = "7.0.0"
    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val schemaSniffer    = "0.0.0"
    val scalaUtil        = "0.1.0"
    val refererParser    = "0.2.3"
    val maxmindIplookups = "0.2.0"
    val json4s           = "3.2.11"
    val igluClient       = "0.2.0"
    val scalaForex       = "0.2.0"
    // Scala (test only)
    val specs2           = "1.14"
    val scalazSpecs2     = "0.1.2"
    val scalaCheck       = "1.10.0"
    val commonsCodec     = "1.5"
    val scalaUri         = "0.4.3"
  }

  object Libraries {
    // Java
    val httpClient       = "org.apache.httpcomponents"  %  "httpclient"                % V.http
    val commonsLang      = "org.apache.commons"         %  "commons-lang3"             % V.commonsLang
    val commonsIo        = "commons-io"                 %  "commons-io"                % V.commonsIo
    val yodaTime         = "joda-time"                  %  "joda-time"                 % V.yodaTime
    val yodaConvert      = "org.joda"                   %  "joda-convert"              % V.yodaConvert
    val useragent        = "bitwalker"                  %  "UserAgentUtils"            % V.useragent
    val jacksonDatabind  = "com.fasterxml.jackson.core" %  "jackson-databind"          % V.jacksonDatabind
    val jsonValidator    = "com.github.fge"             %  "json-schema-validator"     % V.jsonValidator    
    val mavenArtifact    = "org.apache.maven"           %  "maven-artifact"            % V.mavenArtifact
    val uaParser         = "org.clojars.timewarrior"    %  "ua-parser"                 % V.uaParser
    // Scala
    val scalaForex       = "com.snowplowanalytics"      %% "scala-forex"               % V.scalaForex
    val scalaz7          = "org.scalaz"                 %% "scalaz-core"               % V.scalaz7
    val snowplowRawEvent = "com.snowplowanalytics"      %  "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"      %  "collector-payload-1"       % V.collectorPayload
    val schemaSniffer    = "com.snowplowanalytics"      %  "schema-sniffer-1"          % V.schemaSniffer
    val scalaUtil        = "com.snowplowanalytics"      %  "scala-util"                % V.scalaUtil
    val refererParser    = "com.snowplowanalytics"      %% "referer-parser"            % V.refererParser
    val maxmindIplookups = "com.snowplowanalytics"      %% "scala-maxmind-iplookups"   % V.maxmindIplookups
    val json4sJackson    = "org.json4s"                 %% "json4s-jackson"            % V.json4s
    val json4sScalaz     = "org.json4s"                 %% "json4s-scalaz"             % V.json4s    
    val igluClient       = "com.snowplowanalytics"      %  "iglu-scala-client"         % V.igluClient
    val scalaUri         = "com.netaporter"             %% "scala-uri"                 % V.scalaUri
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2"                    % V.specs2         % "test"
    val scalazSpecs2     = "org.typelevel"              %% "scalaz-specs2"             % V.scalazSpecs2   % "test"
    val scalaCheck       = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
    val commonsCodec     = "commons-codec"              %  "commons-codec"             % V.commonsCodec   % "test"
  }
}
