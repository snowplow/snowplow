/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.yodaTime,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
  Dependencies.Libraries.config,
  // Scala
  Dependencies.Libraries.scopt,
  Dependencies.Libraries.scalaz7,
  Dependencies.Libraries.akkaHttp,
  Dependencies.Libraries.akkaSlf4j,
  Dependencies.Libraries.json4sJackson,
  Dependencies.Libraries.snowplowCommonEnrich,
  Dependencies.Libraries.collectorPayload,
  Dependencies.Libraries.pureconfig,
  // Scala (test)
  Dependencies.Libraries.akkaHttpTestkit,
  Dependencies.Libraries.specs2
)

lazy val buildSettings = Seq(
  organization  :=  "com.snowplowanalytics",
  name          :=  "snowplow-stream-collector",
  version       :=  "0.12.0",
  description   :=  "Scala Stream Collector for Snowplow raw events",
  scalaVersion  :=  "2.11.11",
  scalacOptions :=  BuildSettings.compilerOptions,
  scalacOptions in (Compile, console) ~= { _.filterNot(Set("-Ywarn-unused-import")) },
  scalacOptions in (Test, console)    := (scalacOptions in (Compile, console)).value,
  javacOptions  :=  BuildSettings.javaCompilerOptions,
  resolvers     ++= Dependencies.resolutionRepos
)

lazy val allSettings = buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies)

lazy val root = project.in(file("."))
  .settings(buildSettings)
  .aggregate(core, kinesis, pubsub, kafka, nsq, stdout)

lazy val core = project
  .settings(moduleName := "snowplow-stream-collector-core")
  .settings(buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(libraryDependencies ++= commonDependencies)

lazy val kinesis = project
  .settings(moduleName := "snowplow-stream-collector-kinesis")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.kinesis))
  .dependsOn(core)

lazy val pubsub = project
  .settings(moduleName := "snowplow-stream-collector-pubsub")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.pubsub))
  .dependsOn(core)

lazy val kafka = project
  .settings(moduleName := "snowplow-stream-collector-kafka")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.kafkaClients))
  .dependsOn(core)

lazy val nsq = project
  .settings(moduleName := "snowplow-stream-collector-nsq")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.nsqClient))
  .dependsOn(core)

lazy val stdout = project
  .settings(moduleName := "snowplow-stream-collector-stdout")
  .settings(allSettings)
  .dependsOn(core)
