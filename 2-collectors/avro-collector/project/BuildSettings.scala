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

// SBT
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.snowplowanalytics",
    version       := "0.1.0",
    description   := "Snowplow Avro format for raw events emitted by collectors",
    scalaVersion  := "2.9.2", // -> 2.10.0 when Scalding is ready
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    parallelExecution in Test := false, // Parallel tests cause havoc with MapReduce
    logBuffered   := false, // For debugging Specs2 tests
    resolvers     ++= Dependencies.resolutionRepos
  )

  // Add in settings for our SBT-Avro plugin
  import sbtavro.SbtAvro
  lazy val sbtAvroSettings = SbtAvro.avroSettings ++ Seq(
    (version in SbtAvro.avroConfig) := "1.7.4",
    (javaSource in SbtAvro.avroConfig) <<= (sourceManaged in Compile)(_ / "java")
    )

  lazy val buildSettings = basicSettings ++ sbtAvroSettings
}