import AssemblyKeys._

import com.github.bigtoast.sbtthrift.ThriftPlugin

organization := "com.snowplowanalytics"

name := "snowplow-thrift-raw-event"

scalaVersion := "2.10.1"

version := "0.1.0"

crossPaths := false

publishTo <<= version { version =>
  val basePath = "target/repo/%s".format {
    if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases/"
  }
  Some(Resolver.file("Local Maven repository", file(basePath)))
}

seq(ThriftPlugin.thriftSettings: _*)

libraryDependencies += "org.apache.thrift"  %  "libthrift"     % "0.9.1"       % "compile"
