lazy val root = project.in(file("."))
  .settings(
    name := "rdbloader",
    version := "0.12.0-rc1",
    organization := "com.snowplowanalytics",
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.11.8", "2.12.1"),
    initialCommands := "import com.snowplowanalytics.rdbloader._"
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.scalaz7,
      Dependencies.json4s,
      Dependencies.igluClient,
      Dependencies.igluCore,
      Dependencies.circeYaml,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,

      Dependencies.specs2,
      Dependencies.scalazSpecs2,
      Dependencies.scalaCheck
    )
  )
