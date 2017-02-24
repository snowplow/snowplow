/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

lazy val root = project.in(file("."))
  .settings(
    name := "rdbloader",
    version := "0.12.0-rc1",
    organization := "com.snowplowanalytics",
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.11.8", "2.12.1"),
    initialCommands := "import com.snowplowanalytics.rdbloader._"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.scalaz7,
      Dependencies.json4s,
      Dependencies.igluClient,
      Dependencies.igluCore,
      Dependencies.circeYaml,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,

      Dependencies.specs2,
      Dependencies.scalazSpecs2,
      Dependencies.scalaCheck
    )
  )
