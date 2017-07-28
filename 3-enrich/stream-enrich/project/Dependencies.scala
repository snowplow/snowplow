/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
    // For uaParser utils
    "user-agent-parser repo"                 at "https://clojars.org/repo/",
    // For user-agent-utils
    "user-agent-utils repo"                  at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/"
  )

  object V {
    // Java
    val awsSdk               = "1.6.11"
    val kinesisClient        = "1.6.1"
    val kafkaClients         = "0.10.1.0"
    val commonsCodec         = "1.10"
    val config               = "1.0.2"
    val slf4j                = "1.7.5"
    // Scala
    val argot                = "1.0.1"
    val scalazon             = "0.11"
    val scalaz7              = "7.0.0"
    val json4s               = "3.2.11"
    val snowplowRawEvent     = "0.1.0"
    val snowplowCommonEnrich = "0.24.0"
    val igluClient           = "0.4.0"
    val snowplowTracker      = "0.3.0"
    // Scala (test only)
    val specs2               = "2.2.3"
    val scalazSpecs2         = "0.1.2"
  }

  object Libraries {
    // Java
    val awsSdk               = "com.amazonaws"         %  "aws-java-sdk"              % V.awsSdk
    val kinesisClient        = "com.amazonaws"         %  "amazon-kinesis-client"     % V.kinesisClient
    val kafkaClients         = "org.apache.kafka"      %  "kafka-clients"             % V.kafkaClients
    val commonsCodec         = "commons-codec"         %  "commons-codec"             % V.commonsCodec
    val config               = "com.typesafe"          %  "config"                    % V.config
    val slf4j                = "org.slf4j"             %  "slf4j-simple"              % V.slf4j
    val log4jOverSlf4j       = "org.slf4j"             %  "log4j-over-slf4j"          % V.slf4j
    // Scala
    val argot                = "org.clapper"           %% "argot"                     % V.argot
    val scalazon             = "io.github.cloudify"    %% "scalazon"                  % V.scalazon
    val scalaz7              = "org.scalaz"            %% "scalaz-core"               % V.scalaz7
    val json4s               = "org.json4s"            %% "json4s"                    % V.json4s
    val json4sJackson        = "org.json4s"            %% "json4s-jackson"            % V.json4s
    val snowplowRawEvent     = "com.snowplowanalytics" %  "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val igluClient           = "com.snowplowanalytics" %% "iglu-scala-client"         % V.igluClient
    val snowplowCommonEnrich = "com.snowplowanalytics" %  "snowplow-common-enrich"    % V.snowplowCommonEnrich
    val snowplowTracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker"    % V.snowplowTracker
    // Scala (test only)
    val specs2               = "org.specs2"            %% "specs2"                    % V.specs2         % "test"
    val scalazSpecs2         = "org.typelevel"         %% "scalaz-specs2"             % V.scalazSpecs2   % "test"
  }
}
