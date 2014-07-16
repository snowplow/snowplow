/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

object RefererParserBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("referer-parser", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies <++= Dependencies.onVersion(
        all = Seq(
          Libraries.yaml,
          Libraries.httpClient,
          Libraries.scalaCheck,
          Libraries.scalaUtil,
          Libraries.junit,
          Libraries.json,
          Libraries.json4sJackson),
        on29 = Seq(Libraries.specs2._29),
        on210 = Seq(Libraries.specs2._210),
        on211 = Seq(Libraries.specs2._211)
      )
    )
}
