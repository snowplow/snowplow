import AssemblyKeys._

name := "kinesis-s3-sink"

scalaVersion := "2.10.3"

version := "1.0-SNAPSHOT"

resolvers += "snowplow-releases" at "http://maven.snplow.com/releases/"

resolvers += "aws-kinesis-connectors-mvn-repos" at "https://raw.githubusercontent.com/pkallos/amazon-kinesis-connectors/mvn-repo/"

resolvers += "Twitter maven repo" at "http://maven.twttr.com/"

libraryDependencies += "com.amazonaws"              % "amazon-kinesis-client"       % "1.0.0"

libraryDependencies += "com.amazonaws"              % "amazon-kinesis-connector"    % "1.0.0"

libraryDependencies += "com.snowplowanalytics"      % "snowplow-thrift-raw-event"   % "0.1.0"

libraryDependencies += "org.clapper"               %% "argot"                       % "1.0.1"

libraryDependencies += "com.typesafe"               % "config"                      % "1.0.2"

libraryDependencies += "org.slf4j"                  % "slf4j-simple"                % "1.7.6"

libraryDependencies += "com.twitter.elephantbird"   % "elephant-bird-core"          % "3.0.6"

assemblySettings

jarName in assembly := "kinesis-s3-sink.jar"

// Drop these jars
excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set(
    "junit-4.8.2.jar",
    "jsp-2.1-6.1.14.jar",
    "jasper-compiler-5.5.12.jar",
    "jsp-api-2.1-6.1.14.jar",
    "servlet-api-2.5-6.1.14.jar",
    "hadoop-lzo-0.4.16.jar"
  )
  cp filter { jar => excludes(jar.data.getName) }
}
