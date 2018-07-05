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
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),

    libraryDependencies ++= Seq(
      "org.yaml"                  %  "snakeyaml"      % "1.19",
      "org.apache.httpcomponents" %  "httpclient"     % "4.5.3",
      "org.specs2"                %% "specs2"         % specsVersion(scalaVersion.value) % "test",
      "junit"                     %  "junit"          % "4.12"     % "test",
      "org.json"                  %  "json"           % "20170516" % "test",
      "org.json4s"                %% "json4s-jackson" % "3.5.3"    % "test",
      "org.json4s"                %% "json4s-scalaz"  % "3.5.3"    % "test",
      "org.typelevel"             %% "cats-effect"    % "0.10.1"
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
	scmInfo := Some(ScmInfo(url("https://github.com/snowplow/scala-maxmind-iplookups"),
      "scm:git@github.com:snowplow/scala-maxmind-iplookups.git")),
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


def specsVersion(scalaVer: String) = scalaVer match {
  case "2.11.12" => "3.7"
  case "2.12.6" => "2.5"
}
