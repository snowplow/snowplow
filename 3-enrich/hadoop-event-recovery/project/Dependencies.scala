/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Concurrent Maven Repo" at "http://conjars.org/repo" // For Scalding, Cascading etc
  )

  object V {
    // Java
    val hadoop          = "2.4.1"
    val cascading       = "2.7.0"
    val rhino           = "1.7R4"
    // Scala
    val scalding        = "0.15.0"
    val igluClient      = "0.3.2"
    val specs2          = "2.3.11"
    // Add versions for your additional libraries here...
  }

  object Libraries {
    // Java
    val hadoopCommon     = "org.apache.hadoop"          %  "hadoop-common"                % V.hadoop       % "provided"
    val hadoopClientCore = "org.apache.hadoop"          %  "hadoop-mapreduce-client-core" % V.hadoop       % "provided"
    val cascadingCore    = "cascading"                  %  "cascading-core"               % V.cascading
    val cascadingLocal   = "cascading"                  %  "cascading-local"              % V.cascading
    val cascadingHadoop  = "cascading"                  %  "cascading-hadoop2-mr1"        % V.cascading
    val rhino            = "org.mozilla"                % "rhino"                         % V.rhino
    // Scala
    val scaldingCore     = "com.twitter"                %% "scalding-core"           % V.scalding exclude( "cascading", "cascading-local" ) exclude( "cascading", "cascading-hadoop" ) exclude( "cascading", "cascading-hadoop2-mr1" )
    val scaldingArgs     = "com.twitter"                %% "scalding-args"           % V.scalding exclude( "cascading", "cascading-local" ) exclude( "cascading", "cascading-hadoop" ) exclude( "cascading", "cascading-hadoop2-mr1" )
    val scaldingJson = "com.twitter"                %%  "scalding-json"       % V.scalding
     // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"               % V.specs2       % "test"
  }
}
