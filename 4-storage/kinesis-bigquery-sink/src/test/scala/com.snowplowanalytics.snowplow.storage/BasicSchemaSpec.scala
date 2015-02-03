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
class BasicSchemaSpec extends Specification with ValidationMatchers {

  val fields = BasicSchema.fields
  val file = "example_row"

  "BasicSchema.fields" should {
    "be of length 108" in {
      fields.length must beEqualTo(108)
    }
    "have first element ('app_id', 'STRING')" in {
      fields(0) must beEqualTo( ("app_id", "STRING") )
    }
    "have last element ('doc_height', 'INTEGER')" in {
      fields(107) must beEqualTo( ("doc_height", "INTEGER") )
    }
    "have 72 elements with field type 'STRING'" in {
      fields.filter(_._2 == "STRING").length must beEqualTo(72)
    }
    "have 15 elements with field type 'INTEGER'" in {
      fields.filter(_._2 == "INTEGER").length must beEqualTo(15)
    }
    "have 11 elements with field type 'BOOLEAN'" in {
      fields.filter(_._2 == "BOOLEAN").length must beEqualTo(11)
    }
    "have 3 elements with field type 'TIMESTAMP'" in {
      fields.filter(_._2 == "TIMESTAMP").length must beEqualTo(3)
    }
    "have 7 elements with field type 'FLOAT'" in {
      fields.filter(_._2 == "FLOAT").length must beEqualTo(7)
    }
  }

  "BasicScema.names" should {

    val names = BasicSchema.names

    "return object of type Array[String]" in {
      names must haveClass[Array[String]]
    }
    "which is of length 108" in {
      names.length must beEqualTo(108)
    }
    "with first element 'app_id' " in {
      names(0) must beEqualTo("app_id")
    }
    "with 57th element 'se_property' " in {
      names(56) must beEqualTo("se_property")
    }
    "with last element 'doc_height' " in {
      names(107) must beEqualTo("doc_height")
    }
  }

  "BasicScema.types" should {

    val types = BasicSchema.types

    "return object of type Array[String]" in {
      types must haveClass[Array[String]]
    }
    "which is of length 108" in {
      types.length must beEqualTo(108)
    }
    "with first element 'STRING' " in {
      types(0) must beEqualTo("STRING")
    }
    "with 57th element 'STRING' " in {
      types(56) must beEqualTo("STRING")
    }
    "with last element 'INTEGER' " in {
      types(107) must beEqualTo("INTEGER")
    }
  }


}
