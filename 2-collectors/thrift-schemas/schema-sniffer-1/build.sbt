import sbtassembly.AssemblyPlugin.autoImport._
import com.github.bigtoast.sbtthrift.ThriftPlugin

organization := "com.snowplowanalytics"

name := "schema-sniffer-1"

scalaVersion := "2.10.4"

version := "0.0.0"

crossPaths := false

publishTo <<= version { version =>
  val basePath = "target/repo/%s".format {
    if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases/"
  }
  Some(Resolver.file("Local Maven repository", file(basePath)))
}

seq(ThriftPlugin.thriftSettings: _*)

libraryDependencies += "org.apache.thrift"  %  "libthrift"     % "0.9.1"       % "compile"
