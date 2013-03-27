/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
    // For Avro
    val avro         = "1.7.2"
    // Scala (test only)
    val specs2       = "1.12.3" // -> "1.13" when we bump to Scala 2.10.0
    val scalaCheck   = "1.10.0"
    // val useragent = "1.6" No Maven repo, so user-agent-utils is an unmanaged lib
  }

  object Libraries {
    // Avro
    val avro         = "org.apache.avro"            %  "avro"                % V.avro
    // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"              % V.specs2       % "test"
    val scalaCheck   = "org.scalacheck"             %% "scalacheck"          % V.scalaCheck   % "test"
  }
}