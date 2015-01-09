import AssemblyKeys._

import com.github.bigtoast.sbtthrift.ThriftPlugin

organization := "com.snowplowanalytics"

name := "collector-payload-1"

scalaVersion := "2.10.1"

version := "0.0.0"

crossPaths := false

seq(ThriftPlugin.thriftSettings: _*)

libraryDependencies += "org.apache.thrift"  %  "libthrift"     % "0.9.1"       % "compile"
