/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.snowplowanalytics",
    version       := "0.5.5",
    description   := "Hive deserializer for the SnowPlow log data",
    scalaVersion  := "2.9.1",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    parallelExecution in Test := false, // Parallel tests cause havoc with Hive
    resolvers     ++= Dependencies.resolutionRepos
  )

  // Makes our SBT app settings available from within the deserializer
  lazy val javifySettings = Seq(sourceGenerators in Compile <+= (sourceManaged in Compile, version, name, organization) map { (d, v, n, o) =>
    val file = d / "ProjectSettings.java"
    IO.write(file, """package com.snowplowanalytics.snowplow.hadoop.hive.generated;
      |public final class ProjectSettings {
      |  private ProjectSettings() {}
      |  public static final String ORGANIZATION = "%s";
      |  public static final String VERSION = "%s";
      |  public static final String NAME = "%s";
      |}
      |""".stripMargin.format(o, v, n))
    Seq(file)
  })

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    assembleArtifact in packageScala := false,
    jarName in assembly <<= (name, version) { (name, version) => name + "-" + version + ".jar" },
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case x if x.startsWith("META-INF/") => MergeStrategy.discard
        case x => old(x)
      }
    }
  )

  lazy val buildSettings = basicSettings ++ javifySettings ++ sbtAssemblySettings
}