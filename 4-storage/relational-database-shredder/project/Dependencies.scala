/*
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
import sbt._

object Dependencies {
  object V {
    // Java
    val dynamodb        = "1.11.98"
    val jsonValidator   = "2.2.3"
    // Scala
    val spark           = "2.1.0"
    val json4sJackson   = "3.2.11"
    val scalaz7         = "7.0.9"
    val scopt           = "3.5.0"
    val commonEnrich    = "0.25.0-M4"
    val igluClient      = "0.5.0"
    // Scala (test only)
    val specs2          = "2.3.13"
  }

  object Libraries {
    // Java
    val jsonValidator   = "com.github.fge"         % "json-schema-validator"  % V.jsonValidator
    val dynamodb         = "com.amazonaws"         %  "aws-java-sdk-dynamodb" % V.dynamodb
    // Scala
    val sparkCore       = "org.apache.spark"      %% "spark-core"             % V.spark           % "provided"
    val sparkSQL        = "org.apache.spark"      %% "spark-sql"              % V.spark           % "provided"
    val json4sJackson   = "org.json4s"            %% "json4s-jackson"         % V.json4sJackson
    val scalaz7         = "org.scalaz"            %% "scalaz-core"            % V.scalaz7
    val scopt           = "com.github.scopt"      %% "scopt"                  % V.scopt
    val commonEnrich    = "com.snowplowanalytics" %% "snowplow-common-enrich" % V.commonEnrich
    val igluClient      = "com.snowplowanalytics" %% "iglu-scala-client"      % V.igluClient
    // Scala (test only)
    val specs2          = "org.specs2"            %% "specs2-core"            % V.specs2          % "test"
  }
}
