/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
    "Snowplow Analytics Maven repo"          at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    "Spray repo"                             at "http://repo.spray.io",
    "Typesafe repository"                    at "http://repo.typesafe.com/typesafe/releases/",
    // For Scalazon
    "BintrayJCenter"                         at "http://jcenter.bintray.com",
    // For sbt-thrift
    "bigtoast-github"                        at "http://bigtoast.github.com/repo/"
  )

  object V {
    // Java
    val mimepull         = "1.9.4"
    val awsSdk           = "1.6.10"
    val yodaTime         = "2.1"
    val yodaConvert      = "1.2"
    val kafka            = "0.10.2.0"
    // Scala
    val snowplowCommonEnrich = "0.22.0"
    val igluClient       = "0.3.2"
    val scalaz7          = "7.0.0"
    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val spray            = "1.3.3"
    val akka             = "2.3.9"
    val logback          = "1.0.13"
    val commonsCodec     = "1.5"
    val scalazon         = "0.11"
    val argot            = "1.0.1"
    val json4s           = "3.2.11"
    // Scala (test only)
    // Using the newest version of spec (2.3.6) causes
    // conflicts with `spray` for `com.chuusai.shapeless`
    val specs2           = "2.2.3"
  }

  object Libraries {
    // Java
    val mimepull         = "org.jvnet.mimepull"    %  "mimepull"                  % V.mimepull
    val awsSdk           = "com.amazonaws"         %  "aws-java-sdk"              % V.awsSdk
    val yodaTime         = "joda-time"             %  "joda-time"                 % V.yodaTime
    val yodaConvert      = "org.joda"              %  "joda-convert"              % V.yodaConvert
    val kafkaClients     = "org.apache.kafka"      %  "kafka-clients"             % V.kafka

    // Scala
    // Exclude netaporter to prevent conflicting cross-version suffixes for shapeless
    val snowplowCommonEnrich = "com.snowplowanalytics" % "snowplow-common-enrich" % V.snowplowCommonEnrich intransitive
    val igluClient       = "com.snowplowanalytics" %  "iglu-scala-client"         % V.igluClient
    val scalaz7          = "org.scalaz"            %% "scalaz-core"               % V.scalaz7
    val snowplowRawEvent = "com.snowplowanalytics" %  "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics" %  "collector-payload-1"       % V.collectorPayload
    val argot            = "org.clapper"           %% "argot"                     % V.argot
    val sprayCan         = "io.spray"              %% "spray-can"                 % V.spray
    val sprayRouting     = "io.spray"              %% "spray-routing"             % V.spray
    val akkaActor        = "com.typesafe.akka"     %% "akka-actor"                % V.akka
    val akkaSlf4j        = "com.typesafe.akka"     %% "akka-slf4j"                % V.akka
    val logback          = "ch.qos.logback"        %  "logback-classic"           % V.logback
    val commonsCodec     = "commons-codec"         %  "commons-codec"             % V.commonsCodec
    val scalazon         = "io.github.cloudify"    %% "scalazon"                  % V.scalazon
    val json4sJackson    = "org.json4s"            %% "json4s-jackson"            % V.json4s

    // Scala (test only)
    val specs2           = "org.specs2"            %% "specs2"                    % V.specs2   % "test"
    val sprayTestkit     = "io.spray"              %% "spray-testkit"             % V.spray    % "test"
  }
}
