/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    ScalaToolsSnapshots,
    "SnowPlow Analytics Maven repo" at "http://maven.snplow.com/releases/"
  )

  object V {
    val yaml       = "1.10"
    val http       = "4.3.3"
    val specs2     = "1.12.1"
    val scalaCheck = "1.10.0"
    val scalaUtil  = "0.1.0"
  }

  object Libraries {
    val yaml        = "org.yaml"                   %  "snakeyaml"            % V.yaml
    val httpClient  = "org.apache.httpcomponents"  %  "httpclient"           % V.http
    val specs2      = "org.specs2"                 %% "specs2"               % V.specs2      % "test"
    val scalaCheck  = "org.scalacheck"             %% "scalacheck"           % V.scalaCheck  % "test"
    val scalaUtil   = "com.snowplowanalytics"      %  "scala-util"           % V.scalaUtil   % "test"
  }
}