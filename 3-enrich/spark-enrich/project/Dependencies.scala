/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For user-agent-utils
    "Clojars Maven Repo" at "http://clojars.org/repo",
    // For hadoop-lzo
    "Twitter" at "https://maven.twttr.com/"
  )

  object V {
    // Java
    val hadoopLZO        = "0.4.20"
    val elephantBird     = "4.14"
    // Scala
    val spark            = "2.2.0"
    val scalaz7          = "7.0.9"
    val scopt            = "3.5.0"
    val commonEnrich     = "0.32.0-SNAP"
    val igluClient       = "0.5.0"
    // Scala (test only)
    val specs2           = "2.3.13"
    // Thrift (test only)
    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
  }

  object Libraries {
    // Java
    val hadoopLZO        = "com.hadoop.gplcompression" %  "hadoop-lzo"                   % V.hadoopLZO
    val elephantBird     = "com.twitter.elephantbird"  %  "elephant-bird-core"           % V.elephantBird
    // Scala
    val sparkCore        = "org.apache.spark"          %% "spark-core"                   % V.spark        % "provided"
    val sparkSQL         = "org.apache.spark"          %% "spark-sql"                    % V.spark        % "provided"
    val scalaz7          = "org.scalaz"                %% "scalaz-core"                  % V.scalaz7
    val scopt            = "com.github.scopt"          %% "scopt"                        % V.scopt
    val commonEnrich     = "com.snowplowanalytics"     %% "snowplow-common-enrich"       % V.commonEnrich
    val igluClient       = "com.snowplowanalytics"     %% "iglu-scala-client"            % V.igluClient
    // Thrift
    val snowplowRawEvent = "com.snowplowanalytics"     % "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"     % "collector-payload-1"           % V.collectorPayload
    // Scala (test only)
    val specs2           = "org.specs2"                %% "specs2-core"                  % V.specs2           % "test"
  }
}
