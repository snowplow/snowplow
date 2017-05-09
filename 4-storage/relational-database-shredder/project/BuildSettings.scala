/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

object BuildSettings {

  lazy val buildSettings = Seq(
    organization  := "com.snowplowanalytics",
    scalaVersion  := "2.11.11",
    scalacOptions := compilerOptions,
    javacOptions  := javaCompilerOptions,
    parallelExecution in Test := false, // Parallel tests cause havoc with Spark
    oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
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

  lazy val oneJvmPerTestSetting =
    testGrouping in Test := (definedTests in Test).value map { test =>
      Tests.Group(name = test.name, tests = Seq(test), runPolicy = Tests.SubProcess(
        ForkOptions(
          javaHome.value,
          outputStrategy.value,
          Nil,
          Some(baseDirectory.value),
          javaOptions.value,
          connectInput.value,
          envVars.value
        )))
    }

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    // Slightly cleaner jar name
    assemblyJarName in assembly := { name.value + "-" + version.value + ".jar" },
    // Drop these jars
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "minlog-1.2.jar", // Otherwise causes conflicts with Kyro (which bundles it)
        "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
        "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
        "commons-beanutils-1.7.0.jar",      // "
        "hadoop-core-1.1.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
        "hadoop-tools-1.1.2.jar" // "
      )
      cp.filter { jar => excludes(jar.data.getName) }
    },
    assemblyMergeStrategy in assembly := {
      case "project.clj" => MergeStrategy.discard // Leiningen build files
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.endsWith(".html") => MergeStrategy.discard
      case x if x.endsWith("package-info.class") => MergeStrategy.first
      case PathList("com", "google", "common", tail@_*) => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", tail@_*) => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
