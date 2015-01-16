 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

import java.util.{
  ArrayList
}

// Scala
import collection.JavaConversions._
import scala.io.Source

import com.google.api.services.bigquery.model.{
  TableRow,
  TableDataInsertAllRequest,
  TableSchema,
  TableFieldSchema
}

object TSVParser{

  /**
   * @param names - an array of field names. The names must be in order.
   * @param types - an array of field types. The names must be in order.
   * @param file - the location of a TSV list.
   * @return a
   */
  def addFieldsToData(names: Array[String], types: Array[String], file: String): 
    List[List[(String, String, String)]] = {
    for {
        line <- Source.fromFile(file).getLines.toList
        values = getValues(line)
    } yield (names, types, values).zipped.toList
  }

  // TODO: switch from throwing error to using scalaz Validation, maybe.
  def getValues(line: String): List[String] = {
    val values = line.split("\t", -1).toList
    if (values.length != 108){
      throw new Error("There seems to have been a parsing error")
    }
    values
  }

  /**
   * Converts from strings to Any type (of required instance type)
   *
   * @param fieldType - string with one of the following values:
   *    "STRING", "INTEGER", "BOOLEAN", "TIMESTAMP"
   * @param value - string to be converted
   *
   * @return Any with isInstanceOf returning one of 'String', 'Int', or 'Boolean'
   */
  def valueTypeConverter(fieldType: String, value: String): Any = {

    def booleanConverter( bool: String): Boolean = {
      if (bool == "1"){
        true
      } 
      else if (bool == "0") {
        false
      } 
      else {
        bool.toBoolean
      }
    }

    fieldType match {
      case "STRING" => value
      case "INTEGER" => value.toInt
      case "FLOAT" => value.toFloat
      case "BOOLEAN" => booleanConverter(value)
      case "TIMESTAMP" => value
    }
  }

  /**
   * Creates a bigquery schema from an abstract representation.
   *
   * @param abstractSchema - array of pairs of elements, the first element of each pair
   *    must be a field name, and the second must be the fields data type.
   *
   * @return a bigquery TableSchema object.
   */
  def createBigQuerySchema(abstractSchema: Array[(String, String)]): TableSchema = {
    val schemaFieldList = abstractSchema.map(field => {
      val schemaEntry = new TableFieldSchema
      schemaEntry.setName(field._1)
      schemaEntry.setType(field._2)
      schemaEntry
    })
    val schema = new TableSchema
    schema.setFields(schemaFieldList.toList)
  }

  /**
   * makes a bigquery job from a given data list.
   *
   * @param data a list of lists representing the rows to be added, as returned
   *    by addFieldsToData.
   *
   * @return 
   */
  def createUploadData(data: List[List[(String, String, String)]]): TableDataInsertAllRequest = {

    def createRowFromAbstractRow(abstractRow: List[(String, String, String)]): TableDataInsertAllRequest.Rows = {
      val tableRow = new TableRow
      abstractRow.foreach(field => 
            // empty string is a null value
            if (!field._3.isEmpty)  {
              tableRow.set(field._1, valueTypeConverter(field._2, field._3) )
            }
          )
      val row = new TableDataInsertAllRequest.Rows
      row.setJson(tableRow)
    }

    val rowList = data.map(field => 
          createRowFromAbstractRow(field)
        )

    val tableData = new TableDataInsertAllRequest
    tableData.setRows(rowList)
  }

}
