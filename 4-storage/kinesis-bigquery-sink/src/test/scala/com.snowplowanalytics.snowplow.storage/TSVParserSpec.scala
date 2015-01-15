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

import java.util.{
  ArrayList
}

// Scala
import collection.JavaConversions._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

import com.google.api.services.bigquery.model.{
  TableDataInsertAllRequest,
  TableSchema,
  TableFieldSchema
}

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

  "createBigQuerySchema" should {

    val abstractSchema = Array(
        ("abc", "STRING"),
        ("def", "INTEGER"),
        ("ghi", "BOOLEAN")
      )

    val bigQuerySchema = TSVParser.createBigQuerySchema(abstractSchema)

    "return a bigquery TableSchema" in {
      bigQuerySchema must haveClass[TableSchema]
    }

    val schemaFieldList = bigQuerySchema.getFields

    "the same as in abstractSchema" in {
      val schemaArray = schemaFieldList.map(x => (x.getName, x.getType))
      schemaArray.toSet must beEqualTo(abstractSchema.toSet)
    }

  }

  "creatUploadJob" should {

    val data = List(
        List(("abc", "word"), ("def", "123"), ("ghi", "true")), 
        List(("abc", "phrase"), ("def", "456"), ("ghi", "false"))
      )
  
    //"take a list of lists of pairs of strings - similar to that 
    //returned by addFieldsToData"

    val uploadData= TSVParser.createUploadData(data)

    "return a bigquery TableDataInsertAllRequest" in {
      uploadData must haveClass[TableDataInsertAllRequest]
    }

    val rows = uploadData.getRows

    "with correct number of rows" in {
      rows.length must beEqualTo(data.length)
    }

    "with each row containing the correct data" in {
      val rowsZip  = rows zip data
      rowsZip.foreach( rowZip => {
            val jsonTemplate  = "{abc=%s, def=%s, ghi=%s}"
            val jsonText = jsonTemplate.format(rowZip._2(0)._2, rowZip._2(1)._2, rowZip._2(2)._2 )
            rowZip._1.getJson.toString must beEqualTo(jsonText)
      })
      1 must beEqualTo(1) // asserts only in foreach causes problems
    }

  }

}
