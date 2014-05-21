/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/"
  )

  object V {
    // Java
    val hadoop       = "1.1.2"
    // Scala
    val commonEnrich = "0.3.0"
    val scalding     = "0.10.0"
    val scalaz7      = "7.0.0"
    val argonaut     = "6.0.4"
    // Scala (test only)
    val specs2       = "2.3.11"
  }

  object Libraries {
    // Java
    val hadoopCore       = "org.apache.hadoop"          %  "hadoop-core"             % V.hadoop        % "provided"
    // Scala
    val commonEnrich     = "com.snowplowanalytics"      %  "snowplow-common-enrich"  % V.commonEnrich
    val scaldingCore     = "com.twitter"                %% "scalding-core"           % V.scalding
    val scaldingArgs     = "com.twitter"                %% "scalding-args"           % V.scalding
    val scalaz7          = "org.scalaz"                 %% "scalaz-core"             % V.scalaz7
    val argonaut         = "io.argonaut"                %% "argonaut"                % V.argonaut
    // Scala (test only)
    val specs2           = "org.specs2"                 %% "specs2"                  % V.specs2        % "test"
    val specs1           = "org.scala-tools.testing"    %% "specs"                   % V.specs1        % "test"
  }
}
