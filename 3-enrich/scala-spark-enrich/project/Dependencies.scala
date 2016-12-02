/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
    // Required for our json4s snapshot
    "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    // For some misc Scalding and Twitter libs
    "Concurrent Maven Repo" at "http://conjars.org/repo",
    "Clojars Maven Repo" at "http://clojars.org/repo",
    // For Twitter libs
    "Twitter maven libs" at "http://maven.twttr.com/",
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For user-agent-utils
    "user-agent-utils repo" at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/"
  )

  object V {
    // Java
    val hadoop           = "2.4.1"
    // Scala
    val scalaz7          = "7.0.0"
    val commonEnrich     = "0.23.0"
    val igluClient       = "0.4.0"
    val spark            = "2.0.1"
    // Scala (test only)
    val specs2           = "1.14"
    val scalazSpecs2     = "0.1.2"
    val compress         = "0.4.19"
    val commonsCodec     = "1.5"
    // Thrift (test only)
    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
  }

  object Exclusions {
    val mortbayJetty = ExclusionRule(organization = "org.mortbay.jetty")

    val commonsBeanUtil = ExclusionRule(
      organization = "commons-beanutils",
      artifact = "commons-beanutils-core"
    )

    val commonsCollections = ExclusionRule(
      organization = "commons-collections",
      artifact = "commons-collections"
    )

    val commonsCollectionsLogging = ExclusionRule(
      organization = "commons-collections",
      artifact = "commons-logging"
    )

    val esotericMinlog = ExclusionRule(
      organization = "com.esotericsoftware.minlog",
      artifact = "minlog"
    )

    val slf4j = ExclusionRule(
      organization = "org.slf4j",
      artifact = "slf4j-api"
    )
  }

  object Libraries {
    // Scala
    val sparkCore        = ("org.apache.spark"         %%  "spark-core"                  % V.spark)
      .excludeAll(
        Exclusions.mortbayJetty,
        Exclusions.commonsBeanUtil,
        Exclusions.commonsCollections,
        Exclusions.commonsCollectionsLogging,
        Exclusions.esotericMinlog,
        Exclusions.slf4j
      )
    val sparkSql         = ("org.apache.spark"         %%  "spark-sql"                   % V.spark)
    val scalaz7          = "org.scalaz"                %% "scalaz-core"                  % V.scalaz7
    val commonEnrich     = "com.snowplowanalytics"     %  "snowplow-common-enrich"       % V.commonEnrich
    val igluClient       = "com.snowplowanalytics"     %% "iglu-scala-client"            % V.igluClient
    // Scala (test only)
    val specs2           = "org.specs2"                %% "specs2"                       % V.specs2       % "test"
    val scalazSpecs2     = "org.typelevel"             %% "scalaz-specs2"                % V.scalazSpecs2 % "test"
    val compress         = "com.hadoop.gplcompression" %  "hadoop-lzo"                   % V.compress
    val commonsCodec     = "commons-codec"             %  "commons-codec"                % V.commonsCodec % "test"
    // Thrift (test only)
    val snowplowRawEvent = "com.snowplowanalytics"     % "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"     % "collector-payload-1"           % V.collectorPayload
  }
}
