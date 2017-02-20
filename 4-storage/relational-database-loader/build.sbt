name := "relationaldatabaseloader"

organization := "com.snowplowanalytics"

version := "0.1.0-rc1"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.12.1")

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  Dependencies.scopt,
  Dependencies.scalaz7,
  Dependencies.json4s,
  Dependencies.igluClient,
  Dependencies.circeYaml,
  Dependencies.circeGeneric,
  Dependencies.circeGenericExtra,

  Dependencies.specs2,
  Dependencies.scalazSpecs2,
  Dependencies.scalaCheck
)

scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    "-unchecked",
    //"-Ywarn-unused-import",
    "-Ywarn-nullary-unit",
    "-Xfatal-warnings",
    "-Xlint",
    //"-Yinline-warnings",
//    "-Ywarn-dead-code",
    "-Xfuture")

initialCommands := "import com.snowplowanalytics.rdbloader._"

