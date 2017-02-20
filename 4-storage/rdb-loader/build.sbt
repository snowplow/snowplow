/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
    name := "snowplow-rdb-loader",
    version := "0.12.0",
    organization := "com.snowplowanalytics",
    scalaVersion := "2.11.11",
    initialCommands := "import com.snowplowanalytics.snowplow.rdbloader._",
    mainClass in Compile := Some("com.snowplowanalytics.snowplow.rdbloader.Main")
  )

  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.assemblySettings)
  .settings(
    resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases", // For specs2
      "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
    ),
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.scalaz7,
      Dependencies.json4s,
      Dependencies.igluClient,
      Dependencies.igluCore,
      Dependencies.scalaTracker,
      Dependencies.catsFree,
      Dependencies.circeYaml,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,

      Dependencies.postgres,
      Dependencies.s3,

      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
