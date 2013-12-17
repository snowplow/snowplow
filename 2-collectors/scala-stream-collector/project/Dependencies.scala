/*
 * Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    // For scala-util
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/"
  )

  // TODO
  object V {
    /*
    // Java
    val logging    = "1.1.3"
    val httpCore   = "4.3"
    val httpClient = "4.3.1"
    val jacksonCore = "2.3.0"
    // Scala
    val argot      = "1.0.1"
    val config     = "1.0.2"
    val scalaUtil  = "0.1.0"
    // Scala (test only)
    val specs2     = "2.3.4"
    // Add versions for your additional libraries here...
    */
  }

  // TODO
  object Libraries {
    /*
    // Java
    val logging     = "commons-logging"            %  "commons-logging" % V.logging
    val httpCore    = "org.apache.httpcomponents"  %  "httpcore"        % V.httpCore
    val httpClient  = "org.apache.httpcomponents"  %  "httpclient"      % V.httpClient
    val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core"     % V.jacksonCore
    // Scala
    val argot       = "org.clapper"                %% "argot"           % V.argot
    val config      = "com.typesafe"               %  "config"          % V.config
    val scalaUtil   = "com.snowplowanalytics"      %  "scala-util"      % V.scalaUtil
    // Scala (test only)
    val specs2      = "org.specs2"                 %% "specs2"          % V.specs2     % "test"
    // Add additional libraries from mvnrepository.com (SBT syntax) here...
    */
  }
}
