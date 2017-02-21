/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

  object V {
    // Scala
    val scopt            = "3.5.0"
    val scalaz7          = "7.0.9"
    val json4s           = "3.2.11"
    val igluClient       = "0.5.0"
    val circeYaml        = "0.4.0"
    val circe            = "0.7.0"
    val cats             = "0.9.0"
    // Scala (test only)
    val specs2           = "2.3.13"
    val scalazSpecs2     = "0.2"
    val scalaCheck       = "1.12.2"
  }

  // Scala
  val scopt             = "com.github.scopt"           %% "scopt"                     % V.scopt
  val scalaz7           = "org.scalaz"                 %% "scalaz-core"               % V.scalaz7
  val json4s            = "org.json4s"                 %% "json4s-jackson"            % V.json4s
  val igluClient        = "com.snowplowanalytics"      %% "iglu-scala-client"         % V.igluClient
  val cats              = "org.typelevel"              %% "cats"                      % V.cats
  val circeCore         = "io.circe"                   %% "circe-core"                % V.circe
  val circeYaml         = "io.circe"                   %% "circe-yaml"                % V.circeYaml
  val circeGeneric      = "io.circe"                   %% "circe-generic"             % V.circe
  val circeGenericExtra = "io.circe"                   %% "circe-generic-extras"      % V.circe
  // Scala (test only)
  val specs2            = "org.specs2"                 %% "specs2"                    % V.specs2         % "test"
  val scalazSpecs2      = "org.typelevel"              %% "scalaz-specs2"             % V.scalazSpecs2   % "test"
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"                % V.scalaCheck     % "test"
}
