/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
    // For Snowplow and amazon-kinesis-connectors
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
    val slf4j                = "1.7.5"
    val kinesisClient        = "1.2.0"
    val kinesisConnector     = "1.1.1"
    val amazonSdk            = "1.9.16"
    val googleApiClient      = "1.18.0-rc"
    val googleApis           = "v2-rev151-1.18.0-rc"
    val googleOauthClient    = "1.18.0-rc"
    val googleHttpClient     = "1.18.0-rc"
    val googleJackson        = "1.15.0-rc"
    // Scala
    val argot                = "1.0.1"
    val config               = "1.0.2"
    val scalaUtil            = "0.1.0"
    val snowplowCommonEnrich = "0.9.0"
    val scalazon             = "0.5"
    val scalaz7              = "7.0.0"
    val json4s               = "3.2.11"
    // Scala (test only)
    val specs2               = "2.2"
    val scalazSpecs2         = "0.1.2"
    // Scala (compile only)
    val commonsLang3         = "3.1"
  }

  object Libraries {
    // Java
    val googleApis           = "com.google.apis"            % "google-api-services-bigquery"  % V.googleApis
    val googleOauthClient    = "com.google.oauth-client"    % "google-oauth-client"           % V.googleOauthClient
    val googleHttpClient     = "com.google.http-client"     % "google-http-client-jackson2"   % V.googleHttpClient
    val googleApiClient      = "com.google.api-client"      %  "google-api-client"            % V.googleApiClient
    val googleJackson        = "com.google.http-client"     % "google-http-client-jackson"    % V.googleJackson
    val logging              = "commons-logging"            %  "commons-logging"          % V.logging
    val slf4j                = "org.slf4j"                  %  "slf4j-simple"             % V.slf4j
    val kinesisClient        = "com.amazonaws"              %  "amazon-kinesis-client"    % V.kinesisClient
    val kinesisConnector     = "com.amazonaws"              %  "amazon-kinesis-connector" % V.kinesisConnector
    val amazonSdk            = "com.amazonaws"              %  "aws-java-sdk-kinesis"     % V.amazonSdk
    // Scala
    val argot                = "org.clapper"                %% "argot"                    % V.argot
    val config               = "com.typesafe"               %  "config"                   % V.config
    val scalaUtil            = "com.snowplowanalytics"      %  "scala-util"               % V.scalaUtil
    val snowplowCommonEnrich = "com.snowplowanalytics"      % "snowplow-common-enrich"    % V.snowplowCommonEnrich
    val scalazon             = "io.github.cloudify"         %% "scalazon"                 % V.scalazon
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"              % V.scalaz7
    val json4sJackson        = "org.json4s"                 %% "json4s-jackson"           % V.json4s
    // Scala (test only)
    val specs2               = "org.specs2"                 %% "specs2"                   % V.specs2         % "test"
    val scalazSpecs2         = "org.typelevel"              %% "scalaz-specs2"            % V.scalazSpecs2   % "test"
    // Scala (compile only)
    val commonsLang3         = "org.apache.commons"         % "commons-lang3"             % V.commonsLang3   % "compile"
  }
}
