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
    organization  :=  "com.snowplowanalytics",  // TODO: need to update in my fork?
    name          :=  "snowplow-stream-enrich",
    version       :=  "0.13.0-fork",
    description   :=  "The Snowplow Enrichment process, implemented as an Amazon Kinesis app",
    scalaVersion  :=  "2.11.11",
    scalacOptions :=  BuildSettings.compilerOptions,
    scalacOptions in (Compile, console) ~= { _.filterNot(Set("-Ywarn-unused-import")) },
    scalacOptions in (Test, console)    := (scalacOptions in (Compile, console)).value,
    javacOptions  :=  BuildSettings.javaCompilerOptions,
    resolvers     ++= Dependencies.resolutionRepos,
    shellPrompt   := { _ => "stream-enrich> " }
  )
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.kinesisSdk,
      Dependencies.Libraries.dynamodbSdk,
      Dependencies.Libraries.s3Sdk,
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.kafkaClients,
      Dependencies.Libraries.commonsCodec,
      Dependencies.Libraries.config,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.log4jOverSlf4j,
      Dependencies.Libraries.nsqClient,
      // Scala
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.scalaz7,
      Dependencies.Libraries.json4s,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.pureconfig,
      Dependencies.Libraries.snowplowRawEvent,
      Dependencies.Libraries.snowplowCommonEnrich,
      Dependencies.Libraries.igluClient,
      Dependencies.Libraries.snowplowTracker,
      Dependencies.Libraries.snowplowAnalyticsSDK,
      // Test
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.scalacheck
    )
  )
