/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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

    // Scala
    val snowplowRawEvent = "0.1.0"
    val spray            = "1.2.0"
    val akka             = "2.2.3"
    val logback          = "1.0.13"
    val commonsCodec     = "1.5"
    val scalazon         = "0.5"
    val argot            = "1.0.1" 

    // Kafka
    // kafka beta was built in a way that pom generated with a jar isn't valid and maven could not recognize it and parse properly, see :
    // https://issues.apache.org/jira/browse/KAFKA-1064
    val kafka            = "0.8.1"
    val log4j            = "1.2.17"
    val metrics          = "2.2.0"
    val snappy           = "1.1.1.3"

    // Scala (test only)
    // Using the newest version of spec (2.3.6) causes
    // conflicts with `spray` for `com.chuusai.shapeless`
    val specs2           = "2.2.3"
  }

  object Libraries {
    // Java
    val mimepull         = "org.jvnet.mimepull"    %  "mimepull"                  % V.mimepull
    val awsSdk           = "com.amazonaws"         % "aws-java-sdk"               % V.awsSdk

    // Scala
    val snowplowRawEvent = "com.snowplowanalytics" %  "snowplow-thrift-raw-event" % V.snowplowRawEvent
    val argot            = "org.clapper"           %% "argot"                     % V.argot
    val sprayCan         = "io.spray"              %  "spray-can"                 % V.spray
    val sprayRouting     = "io.spray"              %  "spray-routing"             % V.spray
    val akkaActor        = "com.typesafe.akka"     %% "akka-actor"                % V.akka
    val akkaSlf4j        = "com.typesafe.akka"     %% "akka-slf4j"                % V.akka
    val logback          = "ch.qos.logback"        %  "logback-classic"           % V.logback
    val commonsCodec     = "commons-codec"         %  "commons-codec"             % V.commonsCodec
    val scalazon         = "io.github.cloudify"    %% "scalazon"                  % V.scalazon
    val kafka            = "org.apache.kafka"      %% "kafka"                     % V.kafka intransitive()
    val log4j            = "log4j"                 %  "log4j"                     % V.log4j intransitive()
    val metrics          = "com.yammer.metrics"    %  "metrics-core"              % V.metrics
    val snappy           = "org.xerial.snappy"     %  "snappy-java"               % V.snappy

    // Scala (test only)
    val specs2           = "org.specs2"            %% "specs2"                    % V.specs2   % "test"
    val sprayTestkit     = "io.spray"              %  "spray-testkit"             % V.spray    % "test"
  }
}
