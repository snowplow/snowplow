import AssemblyKeys._

import com.github.bigtoast.sbtthrift.ThriftPlugin

organization := "com.snowplowanalytics"

name := "snowplow-thrift-raw-event"

scalaVersion := "2.10.1"

version := "0.1.0"

crossPaths := false

seq(ThriftPlugin.thriftSettings: _*)

libraryDependencies += "org.apache.thrift"  %  "libthrift"     % "0.9.1"       % "compile"
