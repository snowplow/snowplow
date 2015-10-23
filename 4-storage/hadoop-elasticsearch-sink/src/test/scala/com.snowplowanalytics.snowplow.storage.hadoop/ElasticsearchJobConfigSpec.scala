/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.hadoop

// Java
import java.util.Properties

// Scalding
import com.twitter.scalding._

// Specs2 & ScalaCheck
import org.specs2.mutable.{Specification => MutSpecification}
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers
import org.scalacheck._

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests the etlVersion variable.
 * Uses mutable.Specification.
 */
class ElasticsearchJobConfSpec extends MutSpecification with ValidationMatchers {

  "Well-formed Scalding args" should {
    "be converted to a configuration object" in {

      val args = Args(List(
        "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
        "--port", "9200",
        "--host", "localhost",
        "--index", "myindex",
        "--type", "mytype",
        "--input", "/inputdir",
        "--es_nodes_wan_only", "true").mkString(" "))

      val expectedSettings = new Properties
      expectedSettings.setProperty("es.input.json", "true")
      expectedSettings.setProperty("es.nodes.wan.only", "true")

      val expected = ElasticsearchJobConfig(
        "localhost",
        "myindex/mytype",
        9200,
        "/inputdir",
        expectedSettings
        )

      ElasticsearchJobConfig.fromScaldingArgs(args) must beSuccessful(expected)
    }
  }

}
