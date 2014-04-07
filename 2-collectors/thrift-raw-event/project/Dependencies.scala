/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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

  // TODO: both of these can be removed
  val resolutionRepos = Seq(
    // For scala-util
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    // For sbt-thrift
    "bigtoast-github"               at "http://bigtoast.github.com/repo/"
  )

  object V {
    // Scala - compile only for sbt-thrift
    val commonsLang3 = "3.1"
    val thrift       = "0.9.0"

    // Scala - test only
    val specs2       = "2.3.6"
    val scalaCheck   = "1.11.1"
  }

  object Libraries {
    // Scala - compile only for sbt-thrift
    val commonsLang3 = "org.apache.commons" %  "commons-lang3" % V.commonsLang3 % "compile"
    val thrift       = "org.apache.thrift"  %  "libthrift"     % V.thrift       % "compile"

    // Scala - test only
    val specs2       = "org.specs2"         %% "specs2"        % V.specs2       % "test"
    val scalaCheck   = "org.scalacheck"     %% "scalacheck"    % V.scalaCheck   % "test"
  }
}
