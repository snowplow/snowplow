/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
    organization  := "SnowPlow Analytics Ltd",
    version       := "0.4.6",
    description   := "Hive deserializers for the SnowPlow log data",
    scalaVersion  := "2.9.1",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    parallelExecution in Test := false, // Parallel tests cause havoc with Hive
    resolvers     ++= Dependencies.resolutionRepos
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(
    assembleArtifact in packageScala := false,
    target in assembly <<= (target) { (target) => target / ".." / "upload" },
    jarName in assembly <<= (name, version) { (name, version) => name + "-" + version + ".jar" },
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case "META-INF/NOTICE.txt" => MergeStrategy.discard
        case "META-INF/LICENSE.txt" => MergeStrategy.discard
        case x => old(x)
      }
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}
