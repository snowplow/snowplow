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
import Keys._

object Dependencies {
  val resolutionRepos = Seq(
    "SnowPlow Analytics Maven repo" at "http://maven.snplow.com/releases/"
  )

  object V {
    val yaml       = "1.10"
    val http       = "4.3.3"
    object specs2 {
      val _29   = "1.12.1"
      val _210  = "1.14"
      val _211  = "2.3.13"
    }
    val scalaCheck = "1.10.0"
    val scalaUtil  = "0.1.0"
    val junit      = "4.11"
    val json       = "20140107"
    val json4s     = "3.2.9"
  }

  object Libraries {
    val yaml          = "org.yaml"                   %  "snakeyaml"            % V.yaml
    val httpClient    = "org.apache.httpcomponents"  %  "httpclient"           % V.http
    object specs2 {
      val _29   = "org.specs2"                 %% "specs2"               % V.specs2._29        % "test"
      val _210  = "org.specs2"                 %% "specs2"               % V.specs2._210       % "test"
      val _211  = "org.specs2"                 %% "specs2"               % V.specs2._211       % "test"
    }
    val junit         = "junit"                      % "junit"                 % V.junit       % "test"
    val json          = "org.json"                   % "json"                  % V.json        % "test"
    val scalaCheck    = "org.scalacheck"             %% "scalacheck"           % V.scalaCheck  % "test"
    val scalaUtil     = "com.snowplowanalytics"      %  "scala-util"           % V.scalaUtil   % "test"
    val json4sJackson = "org.json4s"                %% "json4s-jackson"        % V.json4s      % "test"
    val json4sScalaz  = "org.json4s"                %% "json4s-scalaz"         % V.json4s      % "test"
  }

  def onVersion[A](all: Seq[A] = Seq(), on29: => Seq[A] = Seq(), on210: => Seq[A] = Seq(), on211: => Seq[A] = Seq()) =
    scalaVersion(v => all ++ (if (v.contains("2.9.")) {
      on29
    } else if (v.contains("2.10.")) {
      on210
    } else {
      on211
    }))
}
