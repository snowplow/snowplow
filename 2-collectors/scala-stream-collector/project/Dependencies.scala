/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    // For uaParser utils
    "user-agent-parser repo" at "https://clojars.org/repo/"
  )

  object V {
    // Java
    val awsSdk               = "1.11.115"
    val pubsub               = "0.37.0-beta"
    val kafka                = "1.0.1"
    val nsqClient            = "1.2.0"
    val yodaTime             = "2.9.9"
    val slf4j                = "1.7.5"
    val config               = "1.3.1"
    // Scala
    val snowplowCommonEnrich = "0.26.0"
    val igluClient           = "0.5.0"
    val collectorPayload     = "0.0.0"
    val scalaz7              = "7.0.9"
    val akkaHttp             = "10.0.9"
    val akkaSlf4j            = "2.4.19"
    val scopt                = "3.6.0"
    val json4s               = "3.2.11"
    val pureconfig           = "0.8.0"
    // Scala (test only)
    val specs2               = "3.9.4"
  }

  object Libraries {
    // Java
    val kinesis              = "com.amazonaws"         %  "aws-java-sdk-kinesis"   % V.awsSdk
    val pubsub               = "com.google.cloud"      %  "google-cloud-pubsub"    % V.pubsub
    val kafkaClients         = "org.apache.kafka"      %  "kafka-clients"          % V.kafka
    val nsqClient            = "com.snowplowanalytics" %  "nsq-java-client"        % V.nsqClient
    val yodaTime             = "joda-time"             %  "joda-time"              % V.yodaTime
    val slf4j                = "org.slf4j"             %  "slf4j-simple"           % V.slf4j
    val log4jOverSlf4j       = "org.slf4j"             %  "log4j-over-slf4j"       % V.slf4j
    val config               = "com.typesafe"          %  "config"                 % V.config

    // Scala
    val snowplowCommonEnrich = "com.snowplowanalytics" %% "snowplow-common-enrich" % V.snowplowCommonEnrich
    val igluClient           = "com.snowplowanalytics" %% "iglu-scala-client"      % V.igluClient
    val collectorPayload     = "com.snowplowanalytics" %  "collector-payload-1"    % V.collectorPayload
    val scalaz7              = "org.scalaz"            %% "scalaz-core"            % V.scalaz7
    val scopt                = "com.github.scopt"      %% "scopt"                  % V.scopt
    val akkaHttp             = "com.typesafe.akka"     %% "akka-http"              % V.akkaHttp
    val akkaSlf4j            = "com.typesafe.akka"     %% "akka-slf4j"             % V.akkaSlf4j
    val json4sJackson        = "org.json4s"            %% "json4s-jackson"         % V.json4s
    val pureconfig           = "com.github.pureconfig" %% "pureconfig"             % V.pureconfig

    // Scala (test only)
    val specs2               = "org.specs2"            %% "specs2-core"            % V.specs2   % "test"
    val akkaHttpTestkit      = "com.typesafe.akka"     %% "akka-http-testkit"      % V.akkaHttp % "test"
  }
}
