import com.typesafe.sbt.SbtGit.GitKeys._

val root = (project in file(".")).
  enablePlugins(ScalaUnidocPlugin, GhpagesPlugin).
  settings(
    name          := "referer-parser",
    organization  := "com.snowplowanalytics",
    version       := "0.3.0",
    description   := "Library for extracting marketing attribution data from referer URLs",
    scalaVersion  := "2.12.6",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    scalacOptions := Seq(
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
      "-Xlint",
      "-Ypartial-unification"
    ),

    libraryDependencies ++= Seq(
      "org.typelevel"             %% "cats-core"          % catsCoreVersion,
      "org.typelevel"             %% "cats-effect"        % catsEffectVersion,
      "io.circe"                  %% "circe-core"         % circeVersion,
      "io.circe"                  %% "circe-generic"      % circeVersion,
      "io.circe"                  %% "circe-parser"       % circeVersion,
      "org.specs2"                %% "specs2-core"        % specs2Version       % "test",
      "org.specs2"                %% "specs2-scalacheck"  % specs2Version       % "test"
    ),

    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
    gitRemoteRepo := "https://github.com/snowplow-referer-parser/jvm-referer-parser.git",
    siteSubdirName := "",

    publishMavenStyle := true,
	publishArtifact := true,
	publishArtifact in Test := false,
	licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
	bintrayOrganization := Some("snowplow"),
	bintrayRepository := "snowplow-maven",
	pomIncludeRepository := { _ => false },
	homepage := Some(url("http://snowplowanalytics.com")),
	scmInfo := Some(ScmInfo(url("https://github.com/snowplow-referer-parser/jvm-referer-parser"),
      "scm:git@github.com:snowplow-referer-parser/jvm-referer-parser.git")),
	pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
            <email>support@snowplowanalytics.com</email>
            <organization>Snowplow Analytics Ltd</organization>
            <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )

val catsCoreVersion = "1.1.0"
val catsEffectVersion = "0.10.1"
val circeVersion = "0.9.3"
val specs2Version = "4.2.0"
