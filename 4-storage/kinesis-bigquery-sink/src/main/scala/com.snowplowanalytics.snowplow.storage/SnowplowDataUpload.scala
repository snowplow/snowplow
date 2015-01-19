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

import java.io.File

object SnowplowDataUploadSetup{
  
  /**
   * Creates a dataset and table with snowplow events schema for the 
   * given project.
   *
   * @param args - command line arguments consisting of:
   *    - ProjectId (must already exist)
   *    - DatasetId (will be created if not prexisting)
   *    - TableId (will be created if not prexisting)
   */
  def main (args: Array[String]){

    require(args.length == 3, 
      """Requires three arguments:
      |  - ProjectId (must already exist)
      |  - DatasetId (will be created if not prexisting)
      |  - TableId (will be created if not prexisting)"""
      .stripMargin
      )
    val projectId = args(0)
    val datasetId = args(1)
    val tableId = args(2)

    //initializes, checks for authorization and if not authorized 
    //starts authorization process
    val bigQueryInterface = new BigqueryInterface(projectId)

    //check if dataset exist

    // creates dataset
    bigQueryInterface.createDataset(datasetId)

    //check if table exists

    //create table
    val schema = TSVParser.createBigQuerySchema( BasicSchema.fields ) 
    bigQueryInterface.createTable(datasetId, schema, tableId)
  }
}

object SnowplowDataUpload{
  
  /**
   * Upload TSV data file to the given database table.
   *
   * @param args - command line arguments consisting of:
   *    - ProjectId (must already exist
   *    - DatabaseId (must already exist)
   *    - TableId (must already exist)
   *    - TSV file location
   */
  def main (args: Array[String]) {
    
    require(args.length == 4, 
      """Requires four arguments:
      |  - ProjectId (must already exist)
      |  - DatasetId (must already exist)
      |  - TableId (must already exist)
      |  - TSV File location"""
      .stripMargin
      )
    val projectId = args(0)
    val datasetId = args(1)
    val tableId = args(2)
    val tsvFileLocation = args(3)
    //
    //initializes, checks for authorization and if not authorized 
    //starts authorization process
    val bigQueryInterface = new BigqueryInterface(projectId)


    def sendBatch (fileName: String) {

      //create bigquery TableDataInsertAllRequest
      val fieldNames = BasicSchema.names
      val fieldTypes = BasicSchema.types
      val dataSet = TSVParser.addFieldsToData(fieldNames, fieldTypes, fileName)
      val tableData = TSVParser.createUploadData(dataSet)

      //send the data
      bigQueryInterface.insertRows(datasetId, tableId, tableData)

    }

    //create array of files to be uploaded
    val fileObj = new File(tsvFileLocation)
    val files =
      if (fileObj.isFile) 
        Array(tsvFileLocation)
      else
        fileObj.list.map(file => tsvFileLocation+"/"+file)

    //send the batches
    for (file <- files) sendBatch(file)
  }
}
