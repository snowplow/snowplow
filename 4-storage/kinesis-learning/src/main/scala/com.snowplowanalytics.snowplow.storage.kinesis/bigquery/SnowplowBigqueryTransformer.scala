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

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

/**
 * Class to convert successfully enriched events to EmitterInputs
 */

class SnowplowBigqueryTransformer(datasetName: String, tableName: String) extends ITransformer[ValidatedRecord, EmitterInput] {

  /**
   * Coverts a kinesis Record into a string.
   */
  private def getRowAsString(record: Record): String = {
        val byteBuffer = record.getData
        val recordBytes = byteBuffer.array
        new String(recordBytes)
  }

  /**
   * @param rawRow a string representing a single row as a tab 
   *    seperated list.
   *
   * @return and ItermediateRecord with each triple of the form
   *    (name, type, value)
   */
  private def makeIntermediateRecord(
    names: Array[String], 
    types: Array[String], 
    rawRow: String 
  ): IntermediateRecord = {
    val values = TsvParser.getValues(rawRow)
    (names, types, values).zipped.toList
  }

  override def toClass(record: Record): IntermediateRecord = {
    
  }

}
