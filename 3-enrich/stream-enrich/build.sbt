/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import com.typesafe.sbt.packager.docker._

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.commonsCodec,
  Dependencies.Libraries.config,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
  Dependencies.Libraries.jacksonDatabind,
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
  // Test
  Dependencies.Libraries.specs2,
  Dependencies.Libraries.scalacheck
)

lazy val buildSettings = Seq(
  organization  :=  "com.snowplowanalytics",
  name          :=  "snowplow-stream-enrich",
  version       :=  "0.22.0",
  description   :=  "The streaming Snowplow Enrichment process",
  scalaVersion  :=  "2.12.10",
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
  daemonUserUid in Docker := None,
  defaultLinuxInstallLocation in Docker := "/home/snowplow", // must be home directory of daemonUser
  dockerUpdateLatest := true
)

lazy val allSettings = buildSettings ++
  BuildSettings.sbtAssemblySettings ++
  Seq(libraryDependencies ++= commonDependencies) ++
  dockerSettings

lazy val root = project.in(file("."))
  .settings(buildSettings)
  .aggregate(core, kinesis, kafka, nsq, stdin, integrationTests)

lazy val core = project
  .settings(moduleName := "snowplow-stream-enrich")
  .settings(buildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(BuildSettings.formatting)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version,
      "commonEnrichVersion" -> Dependencies.V.snowplowCommonEnrich),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.stream.generated"
  )

lazy val kinesis = project
  .settings(moduleName := "snowplow-stream-enrich-kinesis")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/stream-enrich-kinesis")
  .settings(BuildSettings.formatting)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.s3Sdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.jacksonCbor
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val kafka = project
  .settings(moduleName := "snowplow-stream-enrich-kafka")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/stream-enrich-kafka")
  .settings(BuildSettings.formatting)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val nsq = project
  .settings(moduleName := "snowplow-stream-enrich-nsq")
  .settings(allSettings)
  .settings(packageName in Docker := "snowplow/stream-enrich-nsq")
  .settings(BuildSettings.formatting)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.nsqClient))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(core)

lazy val stdin = project
  .settings(moduleName := "snowplow-stream-enrich-stdin")
  .settings(allSettings)
  .settings(BuildSettings.formatting)
  .dependsOn(core)

lazy val integrationTests = project.in(file("./integration-tests"))
  .settings(moduleName := "integration-tests")
  .settings(allSettings)
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(libraryDependencies ++= Seq(
    // Test
    Dependencies.Libraries.kafka,
    Dependencies.Libraries.jinJava
  ))
  .dependsOn(core % "test->test", kafka % "test->compile")
