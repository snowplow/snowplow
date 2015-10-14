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
    "Snowplow Analytics Maven releases repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    "Twitter maven repo"                     at "http://maven.twttr.com/",
    // For Scalazon
    "BintrayJCenter"                         at "http://jcenter.bintray.com",
    "user-agent-utils repo" at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/",
    "clojars" at "http://clojars.org/repo"
  )

  object V {
    // Java
    val logging              = "1.1.3"
    val slf4j                = "1.7.6"
    val kinesisClient        = "1.1.1"
    val kinesisConnector     = "1.1.1"
    val hadoop               = "1.2.1"
    val elephantbird         = "4.5"
    val awssdk         = "1.7.13"
    // Thrift (test only)
    val collectorPayload     = "0.0.0"
    // Scala
    val argot                = "1.0.1"
    val config               = "1.0.2"
    val scalaUtil            = "0.1.0"
    val snowplowCommonEnrich = "0.9.0"
    val scalazon             = "0.5"
    val json4s           = "3.2.11"
    val scalaz7              = "7.0.0"
    // Scala (test only)
    val specs2               = "2.2"
    val scalazSpecs2         = "0.1.2"
    // Scala (compile only)
    val commonsLang3         = "3.1"
    val postgre = "9.3-1103-jdbc41"
    val yodaTime         = "2.2"
    val yodaConvert      = "1.2"
    val hadoopShred = "0.4.0"
  }

  object Libraries {
    // Java
    val awssdk = "com.amazonaws" % "aws-java-sdk" % V.awssdk
    val slf4j                = "org.slf4j"                  %  "slf4j-simple"             % V.slf4j
    val kinesisClient        = "com.amazonaws"              %  "amazon-kinesis-client"    % V.kinesisClient
    val kinesisConnector     = "com.amazonaws"              %  "amazon-kinesis-connector" % V.kinesisConnector
    val hadoop               = "org.apache.hadoop"          %  "hadoop-core"              % V.hadoop
    val elephantbird         = "com.twitter.elephantbird"   %  "elephant-bird-core"       % V.elephantbird
    val postgre               = "org.postgresql"   % "postgresql"                   % V.postgre
    val yodaTime         = "joda-time"                  %  "joda-time"                 % V.yodaTime
    val yodaConvert      = "org.joda"                   %  "joda-convert"              % V.yodaConvert
    val shred               = "com.snowplowanalytics" % "snowplow-hadoop-shred_2.10" % V.hadoopShred
    val jsonPath = "com.jayway.jsonpath" % "json-path" % "2.0.0"
    // Thrift (test only)
    val collectorPayload     = "com.snowplowanalytics"      %  "collector-payload-1"      % V.collectorPayload % "test"
    // Scala
    val argot                = "org.clapper"                %% "argot"                    % V.argot
    val config               = "com.typesafe"               %  "config"                   % V.config
    val scalazon             = "io.github.cloudify"         %% "scalazon"                 % V.scalazon
    val json4sJackson        = "org.json4s"                 %% "json4s-jackson"           % V.json4s
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"              % V.scalaz7
    // Scala (test only)
    val specs2               = "org.specs2"                 %% "specs2"                   % V.specs2         % "test"
    val scalazSpecs2         = "org.typelevel"              %% "scalaz-specs2"            % V.scalazSpecs2   % "test"
    val scalaTest = "org.scalatest" % "scalatest_2.10" % "3.0.0-M9" % "test"
    val scaldi = "org.scaldi" % "scaldi_2.10" % "0.3.2"
    val jetty = "org.eclipse.jetty" % "jetty-server" % "9.2.13.v20150730"
  }
}
