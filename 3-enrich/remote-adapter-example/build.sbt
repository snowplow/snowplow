name := "remote-adapter-example"

version := "0.0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/"
)

libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "7.0.9",

  "com.snowplowanalytics" %% "iglu-scala-client" % "0.5.0",
  "com.snowplowanalytics" %% "snowplow-common-enrich" % "0.35.0"
)
