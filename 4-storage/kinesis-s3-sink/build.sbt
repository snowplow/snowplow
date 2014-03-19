import AssemblyKeys._

name := "kinesis-s3-sink"

scalaVersion := "2.9.1"

version := "1.0-SNAPSHOT"

resolvers += "snowplow-releases" at "http://maven.snplow.com/releases/"

resolvers += "aws-kinesis-connectors-mvn-repos" at "https://raw.githubusercontent.com/pkallos/amazon-kinesis-connectors/mvn-repo/"

resolvers += "Twitter maven repo" at "http://maven.twttr.com/"

libraryDependencies += "com.amazonaws"              % "amazon-kinesis-client"       % "1.0.0"

libraryDependencies += "com.amazonaws"              % "amazon-kinesis-connector"    % "1.0.0"

libraryDependencies += "com.snowplowanalytics"      % "snowplow-thrift-raw-event"   % "0.1.0"

libraryDependencies += "org.clapper"               %% "argot"                       % "0.4"

libraryDependencies += "com.typesafe"               % "config"                      % "1.0.2"

libraryDependencies += "org.slf4j"                  % "slf4j-simple"                % "1.7.6"

libraryDependencies += "org.apache.hadoop"          % "hadoop-core"                 % "0.20.2"

libraryDependencies += "com.hadoop.gplcompression"  % "hadoop-lzo"                  % "0.4.19"

libraryDependencies += "com.twitter.elephantbird"   % "elephant-bird-core"          % "3.0.6"

assemblySettings

jarName in assembly := "kinesis-s3-sink"

