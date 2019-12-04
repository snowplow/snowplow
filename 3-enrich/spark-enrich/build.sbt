/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
lazy val root = project
  .in(file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "snowplow-spark-enrich",
    version := "2.0.0",
    description := "The Snowplow Spark Enrichment process"
  )
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.hadoopLZO,
      Dependencies.Libraries.elephantBird,
      Dependencies.Libraries.geoip2,
      // Scala
      Dependencies.Libraries.sparkCore,
      Dependencies.Libraries.sparkSQL,
      Dependencies.Libraries.decline,
      Dependencies.Libraries.commonEnrich,
      // Scala (test only)
      Dependencies.Libraries.specs2,
      // Thrift (test only)
      Dependencies.Libraries.snowplowRawEvent,
      Dependencies.Libraries.collectorPayload
    )
  )
  .settings(
    buildInfoKeys := BuildSettings.buildInfoSettings,
    buildInfoPackage := BuildSettings.buildInfoPackage
  )

shellPrompt := { _ =>
  "spark-enrich> "
}
