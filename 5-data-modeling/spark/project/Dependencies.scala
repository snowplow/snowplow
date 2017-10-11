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
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/"
  )

  object V {
    // Scala
    val argot                = "1.0.1"
    val spark                = "1.3.0"
    val json4s               = "3.2.10" // See https://github.com/json4s/json4s/issues/212
    val scalaz7              = "7.0.0"
    // Scala (test only)
    val specs2               = "2.2"
    val scalazSpecs2         = "0.1.2"
  }

  object Libraries {
    // Scala
    val argot                = "org.clapper"                %% "argot"                    % V.argot
    val sparkCore            = "org.apache.spark"           %% "spark-core"               % V.spark          % "provided"
    val sparkMllib           = "org.apache.spark"           %% "spark-mllib"              % V.spark          % "provided"
    val sparkSql             = "org.apache.spark"           %% "spark-sql"                % V.spark          % "provided"
    val json4sJackson        = "org.json4s"                 %% "json4s-jackson"           % V.json4s         % "provided"
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"              % V.scalaz7
    // Scala (test only)
    val specs2               = "org.specs2"                 %% "specs2"                   % V.specs2         % "test"
    val scalazSpecs2         = "org.typelevel"              %% "scalaz-specs2"            % V.scalazSpecs2   % "test"
  }
}
