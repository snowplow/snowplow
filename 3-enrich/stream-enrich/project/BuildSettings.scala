/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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

// SBT
import sbt._
import Keys._

object BuildSettings {
  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assemblyJarName in assembly := { s"${moduleName.value}-${version.value}.jar" },
    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("ProjectSettings$.class") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  // Scalafmt plugin
  import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
  lazy val formatting = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := true
  )

  lazy val addExampleConfToTestCp = Seq(
    unmanagedClasspath in Test += baseDirectory.value.getParentFile / "examples"
  )
}
