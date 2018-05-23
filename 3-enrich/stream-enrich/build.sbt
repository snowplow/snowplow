/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

lazy val commonDependencies = Seq(
  // Java
  Dependencies.Libraries.commonsCodec,
  Dependencies.Libraries.config,
  Dependencies.Libraries.slf4j,
  Dependencies.Libraries.log4jOverSlf4j,
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
  version       :=  "0.16.1",
  description   :=  "The streaming Snowplow Enrichment process",
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
  .aggregate(core, kinesis, pubsub, kafka, nsq, stdin, integrationTests)

lazy val core = project
  .settings(moduleName := "snowplow-stream-enrich")
  .settings(buildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version,
      "commonEnrichVersion" -> Dependencies.V.snowplowCommonEnrich),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.stream.generated"
  )

lazy val kinesis = project
  .settings(moduleName := "snowplow-stream-enrich-kinesis")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.s3Sdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.jacksonCbor,
    Dependencies.Libraries.jacksonDatabind
  ))
  .dependsOn(core)

lazy val pubsub = project
  .settings(moduleName := "snowplow-stream-enrich-google-pubsub")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.pubsub,
    Dependencies.Libraries.datastore
  ))
  .dependsOn(core)

lazy val kafka = project
  .settings(moduleName := "snowplow-stream-enrich-kafka")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients
  ))
  .dependsOn(core)

lazy val nsq = project
  .settings(moduleName := "snowplow-stream-enrich-nsq")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(Dependencies.Libraries.nsqClient))
  .dependsOn(core)

lazy val stdin = project
  .settings(moduleName := "snowplow-stream-enrich-stdin")
  .settings(allSettings)
  .dependsOn(core)

lazy val integrationTests = project.in(file("./integration-tests"))
  .settings(moduleName := "integration-tests")
  .settings(allSettings)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(libraryDependencies ++= Seq(
    // Test
    Dependencies.Libraries.embeddedKafka,
    Dependencies.Libraries.jinJava
  ))
  .dependsOn(core % "test->test", kafka % "test->compile")
