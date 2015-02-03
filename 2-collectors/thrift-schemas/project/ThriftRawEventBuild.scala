/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
import sbt._
import Keys._

object ThriftRawEventBuild extends Build {

  import Dependencies._

  // Configure prompt to show current project.
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  lazy val root = Project(id = "root",
    base = file("."),
    aggregate = Seq(snowplowRawEventProject, collectorPayloadProject, schemaSnifferProject),
    settings = Project.defaultSettings ++ Seq(
      publishLocal := {},
      publish := {}
    )
  )

  lazy val snowplowRawEventProject = Project("snowplow-raw-event", file("./snowplow-raw-event"))
    .settings(
      libraryDependencies ++= Seq(
        Libraries.specs2
      )
    )
  lazy val collectorPayloadProject = Project("collector-payload", file("./collector-payload-1"))
  lazy val schemaSnifferProject    = Project("schema-sniffer", file("./schema-sniffer-1"))
}
