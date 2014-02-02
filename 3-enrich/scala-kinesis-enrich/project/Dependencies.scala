/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
    // For Snowplow
    "Snowplow Analytics Maven releases repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For Scalazon
    "BintrayJCenter"                         at "http://jcenter.bintray.com",
    // For user-agent-utils
    "user-agent-utils repo"                  at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/"
  )

  object V {
    // Java
    val logging              = "1.1.3"
    val httpCore             = "4.3"
    val httpClient           = "4.3.1"
    val jacksonCore          = "2.3.0"
    val slf4j                = "1.7.5"
    val awsSdk               = "1.6.11"
    val kinesisClient        = "1.0.0"
    // Scala
    val argot                = "1.0.1"
    val config               = "1.0.2"
    val scalaUtil            = "0.1.0"
    val snowplowRawEvent     = "0.1.0"
    val snowplowCommonEnrich = "0.2.0"
    val scalazon             = "0.5"
    val scalaz7              = "7.0.0"
    val maxmindGeoip         = "0.0.5"
    // Scala (test only)
    val specs2               = "2.3.6"
    val scalazSpecs2         = "0.1.2"
    // Scala (compile only)
    val commonsLang3         = "3.1"
    val thrift               = "0.9.0"
  }

  object Urls {
    val maxmindData = "http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz"
  }

  object Libraries {
    // Java
    val logging              = "commons-logging"            %  "commons-logging"          % V.logging
    val httpCore             = "org.apache.httpcomponents"  %  "httpcore"                 % V.httpCore
    val httpClient           = "org.apache.httpcomponents"  %  "httpclient"               % V.httpClient
    val jacksonCore          = "com.fasterxml.jackson.core" %  "jackson-core"             % V.jacksonCore
    val slf4j                = "org.slf4j"                  %  "slf4j-simple"             % V.slf4j
    val awsSdk               = "com.amazonaws"              %  "aws-java-sdk"             % V.awsSdk
    val kinesisClient        = "com.amazonaws"              %  "amazon-kinesis-client"    % V.kinesisClient
    val maxmindGeoip         = "com.snowplowanalytics"      %% "scala-maxmind-geoip"      % V.maxmindGeoip
    // Scala
    val argot                = "org.clapper"                %% "argot"                    % V.argot
    val config               = "com.typesafe"               %  "config"                   % V.config
    val scalaUtil            = "com.snowplowanalytics"      %  "scala-util"               % V.scalaUtil
    val snowplowRawEvent     = "com.snowplowanalytics"      % "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val snowplowCommonEnrich = "com.snowplowanalytics"      % "snowplow-common-enrich"    % V.snowplowCommonEnrich
    val scalazon             = "io.github.cloudify"         %% "scalazon"                 % V.scalazon
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"              % V.scalaz7
    // Scala (test only)
    val specs2               = "org.specs2"                 %% "specs2"                   % V.specs2         % "test"
    val scalazSpecs2         = "org.typelevel"              %% "scalaz-specs2"            % V.scalazSpecs2   % "test"
    // Scala (compile only)
    val commonsLang3         = "org.apache.commons"         % "commons-lang3"             % V.commonsLang3   % "compile"
    val thrift               = "org.apache.thrift"          % "libthrift"                 % V.thrift         % "compile"
  }
}
