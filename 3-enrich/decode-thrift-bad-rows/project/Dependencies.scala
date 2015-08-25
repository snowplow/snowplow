/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
    "Snowplow Analytics Maven snapshot repo" at "http://maven.snplow.com/snapshots/",
    "Twitter maven repo"                     at "http://maven.twttr.com/",
    // For Scalazon
    "BintrayJCenter"                         at "http://jcenter.bintray.com",
    // For uaParser utils
    "user-agent-parser repo"                 at "https://clojars.org/repo/"
  )

  object V {
    // Scala
    val commonEnrich         = "0.15.0"
  }

  object Libraries {
    // Scala
    val commonEnrich     = "com.snowplowanalytics"     %  "snowplow-common-enrich"       % V.commonEnrich
  }
}
