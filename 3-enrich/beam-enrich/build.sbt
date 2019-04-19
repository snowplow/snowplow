import sbt._
import Keys._

lazy val compilerOptions = Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused-import",
  "-Xfuture"
)

lazy val resolutionRepos = Seq(
  // For Snowplow
  "Snowplow Analytics Maven releases repo" at "http://maven.snplow.com/releases/",
  // For ua-parser
  "user-agent-parser repo"                 at "https://clojars.org/repo/"
)

// we fork a JVM per test in order to not reuse enrichment registries
import Tests._
{
  def oneJVMPerTest(tests: Seq[TestDefinition]) =
    tests.map(t => new Group(t.name, Seq(t), SubProcess(ForkOptions()))).toSeq
  testGrouping in Test := oneJVMPerTest((definedTests in Test).value)
}

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization  := "com.snowplowanalytics",
  version       := "0.2.0",
  scalaVersion  := "2.11.12",
  javacOptions  ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= compilerOptions,
   scalacOptions in (Compile, console) ~= {
    _.filterNot(Set("-Ywarn-unused-import"))
  },
  scalacOptions in (Test, console) ~= {
    _.filterNot(Set("-Ywarn-unused-import"))
  },
  resolvers     ++= resolutionRepos
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

import com.typesafe.sbt.packager.docker._
dockerRepository := Some("snowplow-docker-registry.bintray.io")
dockerUsername := Some("snowplow")
dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0"
maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>"
daemonUser in Docker := "snowplow"

lazy val scioVersion = "0.6.0"
lazy val beamVersion = "2.5.0"
lazy val sceVersion = "0.36.0"
lazy val scalaMacrosVersion = "2.1.0"
lazy val slf4jVersion = "1.7.25"
lazy val scalatestVersion = "3.0.5"

lazy val root: Project = Project(
  "beam-enrich",
  file(".")
).settings(
  commonSettings ++ macroSettings ++ noPublishSettings,
  description := "Streaming enrich job written using SCIO",
  buildInfoKeys := Seq[BuildInfoKey](organization, name, version, "sceVersion" -> sceVersion),
  buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.beam.generated",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "com.snowplowanalytics" %% "snowplow-common-enrich" % sceVersion,
    "org.slf4j" % "slf4j-simple" % slf4jVersion
  ) ++ Seq(
    "com.spotify" %% "scio-test" % scioVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion
  ).map(_ % "test")
).enablePlugins(JavaAppPackaging, BuildInfoPlugin)

lazy val repl: Project = Project(
  "repl",
  file(".repl")
).settings(
  commonSettings ++ macroSettings ++ noPublishSettings,
  description := "Scio REPL for beam-enrich",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-repl" % scioVersion
  ),
  mainClass in Compile := Some("com.spotify.scio.repl.ScioShell")
).dependsOn(
  root
)
