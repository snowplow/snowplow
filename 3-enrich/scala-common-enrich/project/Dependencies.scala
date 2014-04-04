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
    // Required for our Scalaz snapshot
    "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
     // For some misc Scalding and Twitter libs
    "Concurrent Maven Repo" at "http://conjars.org/repo",
     // For Twitter's util functions
    "Twitter Maven Repo" at "http://maven.twttr.com/",
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For user-agent-utils
    "user-agent-utils repo" at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/"
  )

  object V {
    // Java
    val http             = "4.1.1"
    val commonsLang      = "3.1"
    val commonsIo        = "2.4"
    val yodaTime         = "2.1"
    val yodaConvert      = "1.2"
    val useragent        = "1.11"
    // Scala
    val scalaz7          = "7.0.0"
    val argonaut         = "6.0.3"
    val snowplowRawEvent = "0.1.0"
    val scalaUtil        = "0.1.0"
    val refererParser    = "0.1.1"
    val maxmindGeoip     = "0.0.5"
    // Scala (test only)
    val specs2           = "1.14"
    val scalazSpecs2     = "0.1.2"
    val scalaCheck       = "1.10.0"
    val commonsCodec     = "1.5"
  }

  object Urls {
    val maxmindData = "http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz"
  }

  object Libraries {
    // Java
    val httpClient       = "org.apache.httpcomponents" %  "httpclient"                % V.http
    val commonsLang      = "org.apache.commons"        %  "commons-lang3"             % V.commonsLang
    val commonsIo        = "commons-io"                %  "commons-io"                % V.commonsIo
    val yodaTime         = "joda-time"                 %  "joda-time"                 % V.yodaTime
    val yodaConvert      = "org.joda"                  %  "joda-convert"              % V.yodaConvert
    val useragent        = "bitwalker"                 %  "UserAgentUtils"            % V.useragent
    // Scala
    val scalaz7          = "org.scalaz"                %% "scalaz-core"               % V.scalaz7
    val argonaut         = "io.argonaut"               %% "argonaut"                  % V.argonaut
    val snowplowRawEvent = "com.snowplowanalytics"     %  "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val scalaUtil        = "com.snowplowanalytics"     %  "scala-util"                % V.scalaUtil
    val refererParser    = "com.snowplowanalytics"     %  "referer-parser"            % V.refererParser
    val maxmindGeoip     = "com.snowplowanalytics"     %% "scala-maxmind-geoip"       % V.maxmindGeoip
    // Scala (test only)
    val specs2           = "org.specs2"                %% "specs2"                    % V.specs2         % "test"
    val scalazSpecs2     = "org.typelevel"             %% "scalaz-specs2"             % V.scalazSpecs2   % "test"
    val scalaCheck       = "org.scalacheck"            %% "scalacheck"                % V.scalaCheck     % "test"
    val commonsCodec     = "commons-codec"             %  "commons-codec"             % V.commonsCodec   % "test"
  }
}
