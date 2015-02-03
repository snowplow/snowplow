/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.nio.ByteBuffer

// Scala
import collection.JavaConversions._
import scala.io.Source

// AWS
import com.amazonaws.services.kinesis.model.Record

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Test SnowplowBigqueryTransformer
 */
class SnowplowBigqueryTranformerSpec extends Specification with ValidationMatchers {

  val numberOfRowsPerRecord = 108

  val file = "src/test/resources/example_row"

  val snowplowBigqueryTransformer = new SnowplowBigqueryTransformer("abc", "def")

  def makeTestRecords(file: String): List[Record] = {

    def makeTestRecord(rowString: String): Record = {
      val dataBuffer = ByteBuffer.wrap(rowString.getBytes)
      val record = new Record
      record.setData(dataBuffer)
      record
    }

    val source = Source.fromFile(file)
    try{
      val lines = source.getLines.toList
      lines.map{
        line => makeTestRecord(line)
      }
    } finally {
      source.close()
    }
  }

  val testRecords = makeTestRecords(file)

  "toClass" should {
    "return a list of IntermediateRecords" in {
      testRecords.foreach{
        record => {
          val intermediateRecord = snowplowBigqueryTransformer.toClass(record)
          intermediateRecord must haveSuperclass[List[IntermediateRecord]]
        }
      }
      1 must beEqualTo(1)
    }
    "with each intermediateRecord of correct length" in {
      testRecords.foreach{
        record => {
          val intermediateRecord = snowplowBigqueryTransformer.toClass(record)
          intermediateRecord.length must beEqualTo(numberOfRowsPerRecord)
        }
      }
      1 must beEqualTo(1)
    }
  }
}

