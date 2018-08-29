import com.typesafe.sbt.SbtGit.GitKeys._

val root = (project in file(".")).
  enablePlugins(ScalaUnidocPlugin, GhpagesPlugin).
  settings(
    name          := "scala-referer-parser",
    organization  := "com.snowplowanalytics",
    version       := "0.4.0",
    description   := "Library for extracting marketing attribution data from referer URLs",
    scalaVersion  := "2.12.6",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    scalacOptions := BuildSettings.compilerOptions,

    libraryDependencies ++= Seq(
      Dependencies.Libraries.catsCore,
      Dependencies.Libraries.catsEffect,
      Dependencies.Libraries.circeCore,
      Dependencies.Libraries.circeGeneric,
      Dependencies.Libraries.circeParser,
      Dependencies.Libraries.specs2Core,
      Dependencies.Libraries.specs2Scalacheck
    )
  )
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.docSettings)
