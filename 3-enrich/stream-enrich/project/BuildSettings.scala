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

// Scalafmt plugin
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

object BuildSettings {

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assemblyJarName in assembly := { s"${moduleName.value}-${version.value}.jar" }
  )

  lazy val formatting = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := true
  )

  lazy val addExampleConfToTestCp = Seq(
    unmanagedClasspath in Test += baseDirectory.value.getParentFile / "examples"
  )
}
