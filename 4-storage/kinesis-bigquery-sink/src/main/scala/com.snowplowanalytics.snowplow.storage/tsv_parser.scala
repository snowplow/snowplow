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
import org.json4s._
import org.json4s.jackson.JsonMethods._

object TSVParser{

  val jsonSchema = "schema.json"

  /**
   * Takes command line argument a tsv file and parses in to a 
   * list of lists.
   */
  def main (args: Array[String]){
    if (args.length > 0) {
        
      for (line <- Source.fromFile(args(0)).getLines()){
        var split_text = line.split("\t", -1).toList
        if (split_text.length != 108){
          throw new Error("There seems to have been a parsing error")
        }
      }

    } 
    else {
      Console.err.println("Please enter filename")
    }

    println("first element of field: " + VeryBasicSchema.fields(0))
    println("length of array: " + VeryBasicSchema.fields.length)
      
  } 


  /**
   * Gets a json object form a file
   */
  def getJSON (file: String): JValue = {
    val unparsed = Source.fromFile(file).getLines.mkString("\n")
    parse(unparsed)
  }
}
