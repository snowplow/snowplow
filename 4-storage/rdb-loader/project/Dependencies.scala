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
    val scopt            = "3.6.0"
    val scalaz7          = "7.0.9"
    val json4s           = "3.2.11" // evicted by iglu-core with 3.3.0
    val igluClient       = "0.5.0"
    val igluCore         = "0.1.0"
    val scalaTracker     = "0.3.0"
    val circeYaml        = "0.6.1"
    val circe            = "0.8.0"
    val cats             = "0.9.0"

    // Java
    val postgres         = "42.0.0"
    val redshift         = "latest.integration"
    val aws              = "1.10.77"

    // Scala (test only)
    val specs2           = "3.6-scalaz-7.0.7"
    val scalaCheck       = "1.12.6"
  }

  // Scala
  val scopt             = "com.github.scopt"      %% "scopt"                  % V.scopt
  val scalaz7           = "org.scalaz"            %% "scalaz-core"            % V.scalaz7
  val json4s            = "org.json4s"            %% "json4s-jackson"         % V.json4s
  val igluClient        = "com.snowplowanalytics" %% "iglu-scala-client"      % V.igluClient
  val igluCore          = "com.snowplowanalytics" %% "iglu-core"              % V.igluCore     intransitive()
  val scalaTracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker" % V.scalaTracker
  val cats              = "org.typelevel"         %% "cats"                   % V.cats
  val catsFree          = "org.typelevel"         %% "cats-free"              % V.cats
  val circeCore         = "io.circe"              %% "circe-core"             % V.circe
  val circeYaml         = "io.circe"              %% "circe-yaml"             % V.circeYaml
  val circeGeneric      = "io.circe"              %% "circe-generic"          % V.circe
  val circeGenericExtra = "io.circe"              %% "circe-generic-extras"   % V.circe

  // Java
  val postgres          = "org.postgresql"        % "postgresql"               % V.postgres
  val s3                = "com.amazonaws"         % "aws-java-sdk-s3"          % V.aws

  // Scala (test only)
  val specs2            = "org.specs2"            %% "specs2-core"             % V.specs2         % "test"
  val specs2ScalaCheck  = "org.specs2"            %% "specs2-scalacheck"       % V.specs2         % "test"
  val scalaCheck        = "org.scalacheck"        %% "scalacheck"              % V.scalaCheck     % "test"
}
