 /*
 * Copyright (c) 2015 Snowplow Analytics Ltd.
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
import java.io.{
  FileInputStream,
  IOException,
  InputStreamReader,
  FileNotFoundException,
  PrintStream,
  File,
  FileOutputStream
}
import java.util.{
  Arrays,
  ArrayList,
  Collections,
  List,
  Scanner,
  Properties
}
import java.security.PrivateKey

// Scala
import collection.JavaConversions._
import collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//Java Libraries
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleClientSecrets,
  GoogleCredential
}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.auth.oauth2.{
  Credential,
  TokenResponse
}
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeFlow,
  GoogleAuthorizationCodeRequestUrl,
  GoogleClientSecrets,
  GoogleTokenResponse
}
import com.google.api.client.http.{
  HttpTransport,
  FileContent
}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.util.Data
import com.google.api.services.bigquery.{
  Bigquery,
  BigqueryScopes
}
import com.google.api.services.bigquery.model.{
  GetQueryResultsResponse,
  QueryRequest,
  TableDataInsertAllRequest,
  QueryResponse,
  TableDataInsertAllResponse,
  ErrorProto,
  TableCell,
  Dataset,
  DatasetReference,
  Table,
  TableSchema,
  TableFieldSchema,
  TableReference,
  TableRow,
  Job,
  JobConfiguration,
  JobConfigurationLoad
}

// Config
 import com.typesafe.config.Config

// Json
import org.json4s.jackson.JsonMethods.parse

/**
 * Class for uploading and querying Snowplow data on BigQuery
 */

object BigqueryInterface{

  val HttpTransport = new NetHttpTransport
  val JsonFactory = new JacksonFactory

  /* Helper methods */ 

   /**
    * Period between retrying sending events to Elasticsearch
    *
    * @param sleepTime Length of time between tries
    */
   private def sleep(sleepTime: Long): Unit = {
     try {
       Thread.sleep(sleepTime)
     } catch {
       case e: InterruptedException => ()
     }
   }

 /* Oauth methods */

/**
   * Creates the credential object needed for a service account.
   * @see [[https://cloud.google.com/bigquery/authorization#service-accounts google docs]]
   *
   * @param config 
   *
   * @return GoogleCredential
   */
  def getCredentials(config: Config): GoogleCredential = {

    val scopes = Arrays.asList( BigqueryScopes.BIGQUERY )
    val accountId = config.getString( "connector.bigquery.service-account-id" )
    val pkFileLocation = config.getString( "connector.bigquery.service-account-p12file" )
    val pkFile = new File( pkFileLocation )
    if (!pkFile.exists) throw new FileNotFoundException(
      "Private key file not found at location specified at 'service-account-p12file:' in config file."
    )

    new GoogleCredential.Builder().setTransport( HttpTransport ) 
      .setJsonFactory(JsonFactory)
      .setServiceAccountId( accountId )
      .setServiceAccountScopes(scopes)
      .setServiceAccountPrivateKeyFromP12File( pkFile )
      .build()

  }

  /**
   * Checks whether a valid (Http200) response reports errors inserting
   * rows in to the table.
   *
   * @param reponse A TableDataInsertAllResponse object.
   *
   * @returns True if there are errors, else false.
   */
  def checkResponseHasErrors(response: TableDataInsertAllResponse): Boolean = {
    response.getInsertErrors != null
  }

  /**
   * Seperates the bad rows from the good.
   *
   * @param request
   * @param response
   *
   * @return A pair (List[TableRow],List[TableRow]) with first element the failing rows
   *    and second element the good rows,
   */
  def seperateRows(request: TableDataInsertAllRequest, response: TableDataInsertAllResponse): 
  (scala.List[TableDataInsertAllRequest.Rows], scala.List[TableDataInsertAllRequest.Rows]) = {

    // TODO sort out convoluted type coversions.
    // TODO deal with duplicate row errors

    def sortRecurse(
      errorRows: scala.List[TableDataInsertAllRequest.Rows], 
      goodRows: scala.List[TableDataInsertAllRequest.Rows],
      rowsAndErrors: scala.List[(TableDataInsertAllRequest.Rows, ErrorProto)]
    ): (scala.List[TableDataInsertAllRequest.Rows], scala.List[TableDataInsertAllRequest.Rows]) = {
      if (rowsAndErrors.isEmpty){
        (errorRows, goodRows)
      } else {
        rowsAndErrors.head._2.getReason match {
          case "invalid" => 
            sortRecurse( rowsAndErrors.head._1::errorRows, goodRows, rowsAndErrors.tail )
          case "stopped" => 
            sortRecurse( errorRows, rowsAndErrors.head._1::goodRows, rowsAndErrors.tail )
          case error  => throw new RuntimeException(
              "The error message: "+ error + 
              " is unexpected. This is a developer error - create an issue."
            )

        }
      }
    }

    if (!checkResponseHasErrors(response)){
      (scala.List(), request.getRows.asScala.toList)
    } else {
      val requestRows: scala.List[TableDataInsertAllRequest.Rows] = request.getRows.asScala.toList
      val rE = response.getInsertErrors
      val responseErrors: scala.List[ErrorProto] =rE.head.getErrors.asScala.toList
      //basic form of error checking
      if (requestRows.length != responseErrors.length) {
        throw new RuntimeException(
          "List of rows in request is not of same length as list of errors in response. " +
          "Perhaps the request and response are not related?"
        )
      } else {
        val rowsAndErrors = (requestRows, responseErrors).zipped.toList
        sortRecurse(scala.List(), scala.List(), rowsAndErrors)
      }
    }
  }

}

class BigqueryInterface(config: Config) {

  val projectId = config.getString("connector.bigquery.project-number")

  //TODO - describe why next two values are needed - are they
  //used anywhere apart from creating bigquery object?
  val HttpTransport = new NetHttpTransport
  val JsonFactory = new JacksonFactory

  //Need to understand this better
  /**
   * Needed for oauth2 authorization.
   */
  val credentials = BigqueryInterface.getCredentials( config )

  val bigquery = new Bigquery(HttpTransport, JsonFactory, credentials)




   /* Dataset and table creation/deletion methods.*/

  /**
   * Checks whether a given dataset exists
   */
  def checkForDataset(datasetId: String): Boolean = ???

  /**
   * Creates a new dataset.
   *
   * @param datasetId
   */
  def createDataset (datasetId: String) = {
    val dataSet = new Dataset
    val dataSetReference = new DatasetReference
    dataSetReference.setProjectId(projectId)
    dataSetReference.setDatasetId(datasetId)
    dataSet.setDatasetReference(dataSetReference)
    try{
      bigquery.datasets.insert(projectId, dataSet).execute()
      println("Dataset created")
    }catch{
      case ex: IOException =>
        println("IOException while creating " + datasetId + ": " + ex)
    }
  }

  /**
   * Deletes the named dataset.
   *
   * @param datasetId
   */
  def deleteDataset(datasetId: String) = {
    try{
      bigquery.datasets.delete(projectId, datasetId).execute()
      println("Dataset deleted")
    }catch{
      case ex: IOException =>
        println("IOException while deleting" + datasetId + ": " + ex)
    }
  }

  /**
   * Checks whether a given table exists in given dataset
   */
  def checkForTable(datasetId: String, tableId: String): Boolean = ???

  /**
   * Creates a table in the given dataset, with given schema.
   *
   * @param datasetId
   * @param schema
   * @param tableName
   */
  def createTable(datasetId: String, schema: TableSchema, tableName: String) = {

    val table = new Table

    table.setSchema(schema)

    val tableRef = new TableReference
    tableRef.setProjectId(projectId)
    tableRef.setDatasetId(datasetId)
    tableRef.setTableId(tableName)
    table.setTableReference(tableRef)

    try{
      val response = bigquery.tables.insert(projectId, datasetId, table).execute()
      println("Table created")
    }catch{
      case ex: IOException =>
        println("IOException while creating table: " + ex)
    }
  }

  /**
   * Delete the given table in the given dataset.
   */
  def deleteTable(datasetId: String, tableName: String) = {

    try{
      bigquery.tables.delete(projectId, datasetId, tableName).execute()
      println("Table deleted")
    }catch{
      case ex: IOException =>
        println("IOException while deleting table: " + ex)
    }
  }

  /**
   * Insert rows in to the given database.
   *
   * @param datasetId
   * @param tableName
   * @param tableData - TableDataInsertAllRequest object as returned by
   *    TSVPaser.createUploadData
   */
  def insertRows(datasetId: String, tableName: String, tableData: TableDataInsertAllRequest): 
  TableDataInsertAllResponse = {

    // TODO add BackOffPeriod to config file?
    val BackOffPeriod = 10000

    try {
      bigquery.tabledata.insertAll(projectId, datasetId, tableName, tableData).execute()
    } catch {
      case ex: IOException =>
        println("IOException while inserting into table: " + ex)
        BigqueryInterface.sleep(BackOffPeriod)
        insertRows(datasetId, tableName, tableData)
    }
  }

  

  /* Database query methods */

  /**
   * Makes a request to the database and displays the response.
   *
   * @param query [[https://cloud.google.com/bigquery/query-reference bigquery database query]]
   * @param out
   */
  def runQueryRpcAndPrint(query: String, out: PrintStream) {
    val queryRequest = new QueryRequest().setQuery(query)
    val queryResponse = bigquery.jobs.query(projectId, queryRequest).execute()
    
    if (queryResponse.getJobComplete) {
      printRows(queryResponse.getRows, out)
      if (Option(queryResponse.getPageToken).isDefined) {
        return
      }
    }

    while(true) {
      var pageToken: String = null

      val queryResults = bigquery.jobs
        .getQueryResults(projectId, queryResponse.getJobReference.getJobId)
        .setPageToken(pageToken).execute()

      if (queryResults.getJobComplete) {
        printRows(queryResponse.getRows, out)
        pageToken = queryResults.getPageToken

        if (Option(pageToken).isDefined) {
          return
        }
      }
    }
  }

  /**
   * Prints the query response to standard output.
   */
  def printRows(rows: List[TableRow], out: PrintStream){

    def outputCell(cell: TableCell) {
      val cellData = if (Data.isNull(cell.getV)) {
        "Null"
      } else {
        cell.getV.toString
      }
      out.printf("%s, ", cellData)
    }
    if (rows != null) {
      for (row <- rows) {
        for (cell <- row.getF) {
          outputCell(cell)
        }
        out.println
      }
    }
  }

}
