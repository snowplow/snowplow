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

import scala.io.Source

object TSVParser{

  /**
   * Takes command line argument a tsv file and parses in to a 
   * list of lists.
   */
  def main (args: Array[String]){

    val fields_names = BasicSchema.fields.map(_._1)

    if (args.length > 0) {
      val dataset = addFieldsToData(fields_names, args(0))
    } else {
      Console.err.println("Please enter filename")
    }
      
  } 

  /**
   * @param fields - an array of field names. The names must be in order.
   * @param file - the location of a TSV list.
   * @returns a
   */
  def addFieldsToData(fields: Array[String], file: String): List[List[(String, String)]] = {
    for {
        line <- Source.fromFile(file).getLines.toList
        values = getValues(line)
    } yield (fields, values).zipped.toList
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
   * Creates a bigquery job specifying the schema
   * @param schemaList list of pairs of elements, the first element of each pair
   * must be a field name, and the second must be the fields data type.
   * @returns a bigquery Job object ???is this right???
   */
  //def createBigQuerySchema(List[(String, String)]) = ???

  /**
   * @param data a list of lists representing the rows to be added, as returned
   *    by addFieldsToData.
   */
  //def uploadToBigQuery(data: List[List[(String, String)]]): JObject = ???

}
