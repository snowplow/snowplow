/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package storage.kinesis.elasticsearch

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// Snowplow
import enrich.common.utils.ScalazJson4sUtils

/**
 * Tests BadEventTransformer
 */
class BadEventTransformerSpec extends Specification with ValidationMatchers {

  "The from method" should {
    "successfully convert a bad event JSON to an ElasticsearchObject" in {
      val input = """{"line":"failed","errors":["Record does not match Thrift SnowplowRawEvent schema"]}"""
      val result = new BadEventTransformer("snowplow", "bad").fromClass(input -> JsonRecord(input, None).success)
      val elasticsearchObject = result._2.getOrElse(throw new RuntimeException("Bad event failed transformation"))
      elasticsearchObject.getIndex must_== "snowplow"
      elasticsearchObject.getType must_== "bad"
      elasticsearchObject.getSource must_== input
    }
  }

}
