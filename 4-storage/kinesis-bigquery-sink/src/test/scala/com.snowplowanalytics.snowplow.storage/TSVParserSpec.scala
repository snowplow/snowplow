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

package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Test the TSVParser.
 */
class SnowplowTSVParserSpec extends Specification with ValidationMatchers {

  val fields = BasicSchema.fields.map(_._1)
  val file = "example_row"

  "addFieldsToData result" should {
    val result = TSVParser.addFieldsToData(fields, file)
    "have two elements" in {
      result.length must beEqualTo(2)
    }
    "each element should be of length 108" in {
      result(0).length must beEqualTo(108)
      result(1).length must beEqualTo(108)
    }
    "have 0,0 element ('app_id', 'snowplowweb')" in {
      result(0)(0) must beEqualTo( ("app_id", "snowplowweb") )
    }
    "have 1,0 element ('app_id', 'snowplowweb')" in {
      result(1)(0) must beEqualTo( ("app_id", "snowplowweb") )
    }
    "have 0,107 element ('doc_height', '')" in {
      result(0)(107) must beEqualTo( ("doc_height", "") )
    }
    "have 1,107 element ('doc_height', '6015')" in {
      result(1)(107) must beEqualTo( ("doc_height", "6015") )
    }
  }


}
