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
import java.util.{
  ArrayList
}
import java.lang.RuntimeException

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
 * Test the TsvParser.
 */
class SnowplowTsvParserSpec extends Specification with ValidationMatchers {

  val names = SnowplowEnrichedEventSchema.names
  val types = SnowplowEnrichedEventSchema.types
  val file = "src/test/resources/example_row"

  "addFieldsToData result" should {
    val result = TsvParser.addFieldsToData(names, types, file)
    "have two elements" in {
      result.length must beEqualTo(2)
    }
    "each element should be of length 108" in {
      result(0).length must beEqualTo(108)
      result(1).length must beEqualTo(108)
    }
    "have 0,0 element ('app_id', 'snowplowweb')" in {
      result(0)(0) must beEqualTo( ("app_id", "STRING", "snowplowweb") )
    }
    "have 1,0 element ('app_id', 'snowplowweb')" in {
      result(1)(0) must beEqualTo( ("app_id", "STRING", "snowplowweb") )
    }
    "have 0,107 element ('doc_height', '')" in {
      result(0)(107) must beEqualTo( ("doc_height", "INTEGER", "") )
    }
    "have 1,107 element ('doc_height', '6015')" in {
      result(1)(107) must beEqualTo( ("doc_height", "INTEGER", "6015") )
    }
  }

  "createBigQuerySchema" should {

    val abstractSchema = Array(
        ("abc", "STRING"),
        ("def", "INTEGER"),
        ("ghi", "BOOLEAN")
      )

    val bigQuerySchema = TsvParser.createBigQuerySchema(abstractSchema)

    "return a bigquery TableSchema" in {
      bigQuerySchema must haveClass[TableSchema]
    }

    val schemaFieldList = bigQuerySchema.getFields

    "the same as in abstractSchema" in {
      val schemaArray = schemaFieldList.map(x => (x.getName, x.getType))
      schemaArray.toSet must beEqualTo(abstractSchema.toSet)
    }

  }

  "valueTypeConverter" should {
    
    val stringType = ("STRING", "word")
    val integerType = ("INTEGER", "123")
    val floatType = ("FLOAT", "12.34")
    val booleanType = ("BOOLEAN", "true")
    val oneBooleanType = ("BOOLEAN", "1")
    val zeroBooleanType = ("BOOLEAN", "0")
    val timestampType = ("TIMESTAMP", "2014-06-01 14:04:11.639")
    val nonsenseType = ("FOO", "bar")

    "should return string for ('STRING', 'word')" in {
      val returnType = TsvParser.valueTypeConverter(stringType._1, stringType._2).isInstanceOf[String]
      returnType must beEqualTo(true)
    }

    "should return int for ('INTEGER', '123')" in {
      val returnType = TsvParser.valueTypeConverter(integerType._1, integerType._2).isInstanceOf[Int]
      returnType must beEqualTo(true)
    }

    "should return float for ('FLOAT', '12.34')" in {
      val returnType = TsvParser.valueTypeConverter(floatType._1, integerType._2).isInstanceOf[Float]
      returnType must beEqualTo(true)
    }

    "should return boolean for ('BOOLEAN', 'true')" in {
      val returnType = TsvParser.valueTypeConverter(booleanType._1, booleanType._2).isInstanceOf[Boolean]
      returnType must beEqualTo(true)
    }

    "should return boolean for ('BOOLEAN', '1')" in {
      val returnType = TsvParser.valueTypeConverter(oneBooleanType._1, oneBooleanType._2).isInstanceOf[Boolean]
      returnType must beEqualTo(true)
    }

    "should return boolean for ('BOOLEAN', '0')" in {
      val returnType = TsvParser.valueTypeConverter(zeroBooleanType._1, zeroBooleanType._2).isInstanceOf[Boolean]
      returnType must beEqualTo(true)
    }

    "should return string for ('TIMESTAMP', '2014-06-01 14:04:11.639')" in {
      val returnType = TsvParser.valueTypeConverter(timestampType._1, timestampType._2).isInstanceOf[String]
      returnType must beEqualTo(true)
    }
    "should throw a RuntimeException for ('FOO', 'bar')" in {
      TsvParser.valueTypeConverter(nonsenseType._1, nonsenseType._2) should 
        throwA[RuntimeException](message=
          "The string 'FOO' does not represent a valid datatype"
        )
    }
  }

  "createRowFromAbstractRow" should {

    val data = List(
        List(("abc", "STRING", "word"), ("def", "INTEGER", "123"), ("ghi", "BOOLEAN", "true")), 
        List(("abc", "STRING",  "phrase"), ("def", "INTEGER",  "456"), ("ghi", "BOOLEAN",  "false"))
      )
    
    "return a bigquery TableDataInsertAllRequest.Rows" in {
      TsvParser.createRowFromAbstractRow(data(0)) should haveClass[TableDataInsertAllRequest.Rows] 
    }
  }
  "creatUploadData" should {

    val data = List(
        List(("abc", "STRING", "word"), ("def", "INTEGER", "123"), ("ghi", "BOOLEAN", "true")), 
        List(("abc", "STRING",  "phrase"), ("def", "INTEGER",  "456"), ("ghi", "BOOLEAN",  "false"))
      )
  
    //"take a list of lists of pairs of strings - similar to that 
    //returned by addFieldsToData"

    val uploadData= TsvParser.createUploadData(data)

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
            val jsonText = jsonTemplate.format(rowZip._2(0)._3, rowZip._2(1)._3, rowZip._2(2)._3 )
            rowZip._1.getJson.toString must beEqualTo(jsonText)
      })
      1 must beEqualTo(1) // asserts only in foreach causes problems
    }

  }

}
