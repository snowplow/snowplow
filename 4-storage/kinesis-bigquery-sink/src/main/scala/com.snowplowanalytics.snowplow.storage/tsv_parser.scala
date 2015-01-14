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
    if (args.length > 0) {
        
      val dataset: List[List[(String, String)]] = for {
        line <- Source.fromFile(args(0)).getLines.toList
        values = getValues(line)
      } yield (VeryBasicSchema.fields, values).zipped.toList
    } 

    else {
      Console.err.println("Please enter filename")
    }

    println("first element of field: " + VeryBasicSchema.fields(0))
    println("length of array: " + VeryBasicSchema.fields.length)

      
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

}
