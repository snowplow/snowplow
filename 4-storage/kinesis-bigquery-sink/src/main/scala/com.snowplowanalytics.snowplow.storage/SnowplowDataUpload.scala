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
import java.io.File

// Argot
import org.clapper.argot.{
  ArgotParser,
  ArgotUsageException
}
import org.clapper.argot.ArgotConverters._

object SnowplowDataUpload{

  val parser = new ArgotParser("bigquery-loader-cli")
  val createTable = parser.flag[Boolean](
    List("c", "create-table"), 
    "If needed create dataset and table." 
  )
  val projectId = parser.parameter[String](
    "projectId",
    "The project id as obtained from the google developers console.",
    false
  )
  val datasetId = parser.parameter[String](
    "datasetId",
    "Name for the dataset. If create-table flag is not included then this" + 
      " dataset must already exist",
    false
  )
  val tableId = parser.parameter[String](
    "tableId",
    "Name for the table. If create-table flag is not included then this" + 
      " table must already exist",
    false
  )
  val tsvFileLocation = parser.parameter[String](
    "tsvFileLocation",
    "Either the filename of a TSV file, or the name of a directory" +
      " containing TSV files (and only TSV files)",
    false
  )

  
  def main (args: Array[String]) {
    
    try{ 

      parser.parse(args)

      //initializes, checks for authorization and if not authorized 
      //starts authorization process
      val bigqueryInterface = new BigqueryInterface(projectId.value.get)

      if (createTable.value.get) createDatasetAndTable(projectId.value.get, datasetId.value.get, tableId.value.get, bigqueryInterface)

      uploadData(projectId.value.get, datasetId.value.get, tableId.value.get, tsvFileLocation.value.get, bigqueryInterface)

    }
    catch{
      case e: ArgotUsageException => println(e.message) 
    }

  }


  /**
   * Creates a dataset and table with snowplow events schema for the 
   * given project.
   *
   * @param projectId (must already exist)
   * @param datasetId (will be created if not prexisting)
   * @param tableId (will be created if not prexisting)
   */
  def createDatasetAndTable(
    projectId: String, 
    datasetId: String, 
    tableId: String, 
    bigqueryInterface: BigqueryInterface
  ){

    //TODO: check if dataset exist

    // creates dataset
    bigqueryInterface.createDataset(datasetId)

    //TODO: check if table exists

    //create table
    val schema = TSVParser.createBigQuerySchema( BasicSchema.fields ) 
    bigqueryInterface.createTable(datasetId, schema, tableId)
  }


  /**
   * Upload TSV data file to the given database table.
   *
   * @param projectId (must already exist
   * @param databaseId (must already exist)
   * @param tableId (must already exist)
   * @param fileLocation Either the filename of a TSV file, or the name of a directory 
   *      containing TSV files (and only TSV files)
   */
  def uploadData (
    projectId: String, 
    datasetId: String, 
    tableId: String, 
    fileLocation: String, 
    bigqueryInterface: BigqueryInterface
  ){

    def sendBatch (fileName: String) {

      //create bigquery TableDataInsertAllRequest
      val fieldNames = BasicSchema.names
      val fieldTypes = BasicSchema.types
      val dataSet = TSVParser.addFieldsToData(fieldNames, fieldTypes, fileName)
      val tableData = TSVParser.createUploadData(dataSet)

      //send the data
      bigqueryInterface.insertRows(datasetId, tableId, tableData)

    }

    //create array of files to be uploaded
    val fileObj = new File(fileLocation)
    val files =
      if (fileObj.isFile) 
        Array(fileLocation)
      else
        fileObj.list.map(file => fileLocation+"/"+file)

    //send the batches
    for (file <- files) sendBatch(file)

  }

}
