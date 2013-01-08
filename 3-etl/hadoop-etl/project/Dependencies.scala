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
    ScalaToolsSnapshots,
    "Concurrent Maven Repo" at "http://conjars.org/repo" // For Scalding, Cascading etc
  )

  object V {
    val scalding    = "0.7.3"
    val http        = "4.1.1"
    val specs2      = "1.8"
    val yodaTime    = "2.1"
    val yodaConvert = "1.2"
    val specs       = "1.6.6" // TODO: remove
  }

  object Libraries {
    val scalding    = "com.twitter"                %% "scalding"            % V.scalding
    // val httpCore    = "org.apache.httpcomponents"  %  "httpcore"            % V.http  
    val httpClient  = "org.apache.httpcomponents"  %  "httpclient"          % V.http
    val yodaTime    = "joda-time"                  %  "joda-time"           % V.yodaTime
    val yodaConvert = "org.joda"                   %  "joda-convert"        % V.yodaConvert
    val specs2      = "org.specs2"                 %% "specs2"              % V.specs2       % "test"
    val specs       = "org.scala-tools.testing"    %% "specs"               % V.specs        % "test" // TODO: remove
  }
}