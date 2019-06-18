/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the
 * Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.  See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 */

// SBT
import sbt._
import Keys._

object BuildSettings {
  // sbt-assembly settings for building an executable
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assemblyJarName in assembly := { s"${moduleName.value}-${version.value}.jar" },
    // merge strategy for fixing netty conflict
    assemblyMergeStrategy in assembly := {
      case PathList("io", "netty", xs @ _*) => MergeStrategy.first
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
