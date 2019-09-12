/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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

import com.typesafe.sbt.packager.docker._

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.yodaTime,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
  Dependencies.Libraries.config,
  Dependencies.Libraries.prometheus,
  Dependencies.Libraries.prometheusCommon,
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
  version       :=  "0.16.0",
  description   :=  "Scala Stream Collector for Snowplow raw events",
  scalaVersion  :=  "2.11.11",
  scalacOptions :=  BuildSettings.compilerOptions,
  scalacOptions in (Compile, console) ~= { _.filterNot(Set("-Ywarn-unused-import")) },
  scalacOptions in (Test, console)    := (scalacOptions in (Compile, console)).value,
  javacOptions  :=  BuildSettings.javaCompilerOptions,
  resolvers     ++= Dependencies.resolutionRepos
)

lazy val dockerSettings = Seq(
  maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
  dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
  daemonUser in Docker := "snowplow",
  dockerUpdateLatest := true
)

lazy val allSettings = buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies) ++
  dockerSettings

lazy val root = project.in(file("."))
  .settings(buildSettings)
  .aggregate(core, kinesis, pubsub, kafka, nsq, stdout)

lazy val core = project
  .settings(moduleName := "snowplow-stream-collector-core")
  .settings(buildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, "shortName" -> "ssc", scalaVersion),
    buildInfoPackage := "com.snowplowanalytics.snowplow.collectors.scalastream.generated"
  )

lazy val kinesis = project
  .settings(moduleName := "snowplow-stream-collector-kinesis")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/scala-stream-collector-kinesis")
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.kinesis))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val pubsub = project
  .settings(moduleName := "snowplow-stream-collector-google-pubsub")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/scala-stream-collector-pubsub")
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.pubsub))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val kafka = project
  .settings(moduleName := "snowplow-stream-collector-kafka")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/scala-stream-collector-kafka")
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.kafkaClients))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val nsq = project
  .settings(moduleName := "snowplow-stream-collector-nsq")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/scala-stream-collector-nsq")
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.nsqClient))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val stdout = project
  .settings(moduleName := "snowplow-stream-collector-stdout")
  .settings(allSettings)
  .dependsOn(core)
