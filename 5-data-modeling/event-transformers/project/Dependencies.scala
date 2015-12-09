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

  val resolutionRepos = Seq()

  object V {
    // Java
    val slf4j                = "1.7.13"
    // Scala
    val json4s               = "3.2.10" // See https://github.com/json4s/json4s/issues/212
    val scalaz7              = "7.0.0"
    // Scala (test only)
  }

  object Libraries {
    // Java
    val slf4j                = "org.slf4j"                  %  "slf4j-log4j12"            % V.slf4j          % "provided"
    // Scala
    val json4sJackson        = "org.json4s"                 %% "json4s-jackson"           % V.json4s         % "provided"
    val scalaz7              = "org.scalaz"                 %% "scalaz-core"              % V.scalaz7
    // Scala (test only)
  }
}
