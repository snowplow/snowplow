/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
import sbt._
import Keys._
import sbtbuildinfo.BuildInfoKey

// Scalafmt plugin
import com.lucidchart.sbt.scalafmt.ScalafmtPlugin._
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq(
    organization  := "com.snowplowanalytics",
    scalaVersion  := "2.12.10",
    scalacOptions := compilerOptions,
    javacOptions  := javaCompilerOptions,
    parallelExecution in Test := false, // Parallel tests cause havoc with Spark
    resolvers     ++= Dependencies.resolutionRepos
  )

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
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  lazy val buildInfoSettings = Seq [BuildInfoKey](
    organization,
    name,
    version,
    "commonEnrichVersion" -> Dependencies.V.commonEnrich
  )

  lazy val buildInfoPackage = "com.snowplowanalytics.snowplow.enrich.spark.generated"

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    // Slightly cleaner jar name
    assemblyJarName in assembly := { name.value + "-" + version.value + ".jar" },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename(
        // Fix incompatibility with IAB client
        "org.apache.commons.io.**" -> "shadecommonsio.@1"
      ).inAll
    ),
    assemblyMergeStrategy in assembly := {
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.endsWith(".html") => MergeStrategy.discard
      case x if x.endsWith("package-info.class") => MergeStrategy.first
      case PathList("com", "google", "common", tail@_*) => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", tail@_*) => MergeStrategy.first
      case "build.properties" => MergeStrategy.first
      case "module-info.class" => MergeStrategy.first // for joda-money-1.0.1.jar and joda-convert-2.2.0.jar
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
  lazy val formatting = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := true,
    scalafmtVersion   := "1.3.0"
  )
}
