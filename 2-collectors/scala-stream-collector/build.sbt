/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
lazy val root = project.in(file("."))
  .settings(
    name        := "snowplow-stream-collector",
    version     := "0.9.0",
    description := "Scala Stream Collector for Snowplow raw events"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.Libraries.awsSdk,
      Dependencies.Libraries.kafkaClients,
      Dependencies.Libraries.yodaTime,
      Dependencies.Libraries.logback,
      Dependencies.Libraries.commonsCodec,
      // Scala
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.scalaz7,
      Dependencies.Libraries.sprayCan,
      Dependencies.Libraries.sprayRouting,
      Dependencies.Libraries.akkaActor,
      Dependencies.Libraries.akkaSlf4j,
      Dependencies.Libraries.json4sJackson,
      Dependencies.Libraries.snowplowCommonEnrich,
      Dependencies.Libraries.collectorPayload,
      // Scala (test)
      Dependencies.Libraries.sprayTestkit,
      Dependencies.Libraries.specs2
    )
  )

shellPrompt := { _ => "stream-collector> "}