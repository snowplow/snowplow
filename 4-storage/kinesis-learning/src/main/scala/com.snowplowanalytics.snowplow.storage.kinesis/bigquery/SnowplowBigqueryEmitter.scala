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

// Java
import java.io.{File, FileNotFoundException}

// Scala
import collection.JavaConversions._

// Config
 import com.typesafe.config.{Config, ConfigFactory}

// Amazon Kinesis
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Google BigQuery
import com.google.api.services.bigquery.model.TableRow

/**
 * Helper methods for SnowplowBigqueryEmitter
 */
object SnowplowBigqueryEmitter {

  /**
   * @param fileName location of a HOCON file
   *
   * @return typesafe Config object
   */
  def getConfigFromFile(fileName: String):Config = {
    val file = new File(fileName)
    if (file.exists) {
      ConfigFactory.parseFile(file)
    }else{
      throw new FileNotFoundException
    }
  } 

}
/**
 * Class to send records to BigQuery
 */
class SnowplowBigqueryEmitter(configuration: KinesisConnectorConfiguration)
extends IEmitter[IntermediateRecord]{
  
  val config = SnowplowBigqueryEmitter.getConfigFromFile("application.conf")
  val projectNumber = config.getString("connector.bigquery.project-number")
  val datasetName = config.getString("connector.bigquery.dataset-name")
  val tableName = config.getString("connector.bigquery.table-name")
  val schema = TsvParser.createBigQuerySchema(SnowplowEnrichedEventSchema.fields)

  val bigqueryInterface = new BigqueryInterface( config )

  def emit(buffer: com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer[IntermediateRecord]): 
  java.util.List[IntermediateRecord] = {
      
    // TODO - creating dataset and tables should only be called once.
    // create the dataset
    bigqueryInterface.createDataset(datasetName)
    // create the table
    bigqueryInterface.createTable(datasetName, schema, tableName)

    // create bigquery TableDataInsertAllRequest
    val records = buffer.getRecords.toList
    val dataToUpload = TsvParser.createUploadData(records)

    //send the data
    bigqueryInterface.insertRows(datasetName, tableName, dataToUpload)
    List()
  }

  def fail(x$1: java.util.List[IntermediateRecord]): Unit = ???

 def shutdown(): Unit = ???

}
