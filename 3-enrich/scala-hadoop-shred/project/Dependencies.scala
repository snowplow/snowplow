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
    "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
    // For Snowplow libs
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    // For user-agent-utils
    "user-agent-utils repo" at "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/",
    // For uaParser utils
    "user-agent-parser repo" at "https://clojars.org/repo/"
  )

  object V {
    // Java
    val hadoop          = "1.1.2"
    // val commonsLang      = "3.1"
    val jacksonDatabind = "2.2.3"
    val jsonValidator   = "2.2.3"
    val yodaTime        = "2.1"
    val yodaConvert     = "1.2"
    // Scala
    val json4sJackson   = "3.2.11"
    val commonEnrich    = "0.15.0"
    val scalding        = "0.10.0"
    val scalaz7         = "7.0.0"
    val igluClient      = "0.3.0"
    // Scala (test only)
    val specs2          = "1.14" // Downgrade to prevent issues in job tests. WAS: "2.3.11"
    val scalazSpecs2    = "0.1.2"
  }

  object Libraries {
    // Java
    val hadoopCore       = "org.apache.hadoop"          %  "hadoop-core"             % V.hadoop          % "provided"
    // val commonsLang      = "org.apache.commons"         %  "commons-lang3"           % V.commonsLang
    val jacksonDatabind  = "com.fasterxml.jackson.core" %  "jackson-databind"        % V.jacksonDatabind
    val jsonValidator    = "com.github.fge"             %  "json-schema-validator"   % V.jsonValidator
    val yodaTime         = "joda-time"                  %  "joda-time"               % V.yodaTime
    val yodaConvert      = "org.joda"                   %  "joda-convert"            % V.yodaConvert
    // Scala
    val json4sJackson    = "org.json4s"                 %% "json4s-jackson"          % V.json4sJackson
    val commonEnrich     = "com.snowplowanalytics"      %  "snowplow-common-enrich"  % V.commonEnrich
    val scaldingCore     = "com.twitter"                %% "scalding-core"           % V.scalding
    val scaldingArgs     = "com.twitter"                %% "scalding-args"           % V.scalding
    // val scaldingJson     = "com.twitter"                %% "scalding-json"           % V.scalding
    val scalaz7          = "org.scalaz"                 %% "scalaz-core"             % V.scalaz7
    val igluClient       = "com.snowplowanalytics"      %  "iglu-scala-client"       % V.igluClient
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2"                  % V.specs2          % "test"
    val scalazSpecs2     = "org.typelevel"              %% "scalaz-specs2"           % V.scalazSpecs2    % "test"
  }
}
