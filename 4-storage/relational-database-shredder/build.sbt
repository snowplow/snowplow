/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */

lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-relational-database-shredder",
    version     := "0.12.0-rc1",
    description := "Spark job to shred event and context JSONs from Snowplow enriched events"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.jsonValidator,
      Dependencies.Libraries.dynamodb,
      // Scala
      Dependencies.Libraries.sparkCore,
      Dependencies.Libraries.sparkSQL,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.scalaz7,
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.commonEnrich,
      Dependencies.Libraries.igluClient,
      // Scala (test only)
      Dependencies.Libraries.specs2
    )
  )

shellPrompt := { _ =>  "relational-database-shredder> " }
