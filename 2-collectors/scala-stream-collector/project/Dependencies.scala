/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
    val awsSdk               = "1.11.573"
    val pubsub               = "0.37.0-beta"
    val kafka                = "2.1.1"
    val nsqClient            = "1.2.0"
    val yodaTime             = "2.9.9"
    val slf4j                = "1.7.5"
    val config               = "1.3.1"
    val prometheus           = "0.5.0"
    // Scala
    val collectorPayload     = "0.0.0"
    val scalaz7              = "7.0.9"
    val akkaHttp             = "10.1.10"
    val akka                 = "2.5.23"
    val scopt                = "3.6.0"
    val pureconfig           = "0.11.1"
    val json4s               = "3.2.11"
    val badRows              = "0.1.0"
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
    val prometheus           = "io.prometheus"         %  "simpleclient"           % V.prometheus
    val prometheusCommon     = "io.prometheus"         %  "simpleclient_common"    % V.prometheus

    // Scala
    val collectorPayload     = "com.snowplowanalytics" %  "collector-payload-1"    % V.collectorPayload
    val badRows              = "com.snowplowanalytics" %% "snowplow-badrows"       % V.badRows
    val scopt                = "com.github.scopt"      %% "scopt"                  % V.scopt
    val akkaHttp             = "com.typesafe.akka"     %% "akka-http"              % V.akkaHttp
    val akkaStream           = "com.typesafe.akka"     %% "akka-stream"            % V.akka
    val akkaSlf4j            = "com.typesafe.akka"     %% "akka-slf4j"             % V.akka
    val json4sJackson        = "org.json4s"            %% "json4s-jackson"         % V.json4s
    val pureconfig           = "com.github.pureconfig" %% "pureconfig"             % V.pureconfig

    // Scala (test only)
    val specs2               = "org.specs2"            %% "specs2-core"            % V.specs2   % Test
    val akkaTestkit          = "com.typesafe.akka"     %% "akka-testkit"           % V.akka     % Test
    val akkaHttpTestkit      = "com.typesafe.akka"     %% "akka-http-testkit"      % V.akkaHttp % Test
  }
}
