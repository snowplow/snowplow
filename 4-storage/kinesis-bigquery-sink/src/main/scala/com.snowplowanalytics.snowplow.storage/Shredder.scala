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

// Scala
import collection.JavaConversions._
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

/**
 * Authenticates the user and makes a query to the wikipedia database
 */
object bigQueryAuth {

  /**
   * Obtained from the Google developers console.
   */
  val ProjectId = "742196692985" // Add projectId here

  /**
   * Location of the client secrets. This file is obtained from
   * the Google developers console.
   */
  val ClientSecretsLocation = "client_secrets.json"
  val HttpTransport = new NetHttpTransport
  val JsonFactory = new JacksonFactory

  /**
   * Handles OAuth authorization and makes a single query to the public 
   * wikipedia sample database
   */
  //def main(args: Array[String]) {
   
    //val credentials = {
      //val fis = new FileInputStream(ClientSecretsLocation)
      //val reader = new InputStreamReader(fis)
      //val gcs = GoogleClientSecrets.load(new JacksonFactory, reader)
      //getCredentials(gcs)
    //}
	 
    //val bigquery = new Bigquery(HttpTransport, JsonFactory, credentials)
    //val query = "SELECT TOP( title, 10) as title, COUNT(*) as revision_count " +
      //"FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;"
    //runQueryRpcAndPrint(bigquery, ProjectId, query, System.out);
  //}
  
  //TODO
  //main should:  authenticate to bigquerytest project (project_id: ac-bigquery-tests, project_no: 742196692985) x
  //              create dataset x
  //              create table x
  //              add row to table x
  //              add row using json
  //              query table
  //              remove row from table (not possible)
  //              delete table x
  //              delete dataset x

  //def main(args: Array[String]){

    //val credentials = loadCredentials
    
    //storeRefreshToken(credentials.getRefreshToken())

    //val bigquery = new Bigquery(HttpTransport, JsonFactory, credentials)
    
    //createDataset(ProjectId, bigquery)
    //createTable(ProjectId, bigquery)
    //addRow(ProjectId, bigquery)
    ////addRowsFromFile(ProjectId, bigquery)
    //deleteTable(ProjectId, bigquery)
    //deleteDataset(ProjectId, bigquery)
  //}

  def loadCredentials: Credential = {

    loadRefreshToken match {
      case Some(s) => createCredentialWithRefreshToken(HttpTransport, JsonFactory, new TokenResponse().setRefreshToken(s))
      case None => getCredentials
    }
    
  }

  /**
   * Checks if given dataset already exists.
   */
  //def checkForDataset(projectId: String, bigquery: Bigquery, datasetId: String): Boolean = {
    //val datasets = bigquery.datasets.list(projectId).execute()
    
  //}

  /**
   * Creates a dataset with name testdataset.
   */
  def createDataset (projectId: String, bigquery: Bigquery) = {
    val dataSetId = "testdataset"
    val dataSet = new Dataset
    val dataSetReference = new DatasetReference
    dataSetReference.setProjectId(projectId)
    dataSetReference.setDatasetId(dataSetId)
    dataSet.setDatasetReference(dataSetReference)
    try{
      bigquery.datasets.insert(projectId, dataSet).execute()
      println("Dataset created")
    }catch{
      case ex: IOException =>
        println("IOException while creating " + dataSetId + ": " + ex)
    }
  }

  /**
   * Deletes the dataset with name testdatatable
   */
  def deleteDataset(projectId: String, bigquery: Bigquery) = {
    val dataSetId = "testdataset"
    try{
      bigquery.datasets.delete(projectId, dataSetId).execute()
      println("Dataset deleted")
    }catch{
      case ex: IOException =>
        println("IOException while deleting" + dataSetId + ": " + ex)
    }
  }

  /**
   * Creates a table with fields firstname, lastname, age.
   */
  def createTable(projectId: String, bigquery: Bigquery) = {

    val dataSetId = "testdataset"

    val tableName = "testtable"
    val table = new Table

    val schema = new TableSchema
    val tableFieldList = new ArrayList[TableFieldSchema]

    val schemaEntryFirstName = new TableFieldSchema
    schemaEntryFirstName.setName("firstName")
    schemaEntryFirstName.setType("STRING")
    tableFieldList.add(schemaEntryFirstName)

    val schemaEntryLastName = new TableFieldSchema
    schemaEntryLastName.setName("lastName")
    schemaEntryLastName.setType("STRING")
    tableFieldList.add(schemaEntryLastName)

    val schemaEntryAge = new TableFieldSchema
    schemaEntryAge.setName("age")
    schemaEntryAge.setType("INTEGER")
    tableFieldList.add(schemaEntryAge)

    schema.setFields(tableFieldList)
    table.setSchema(schema)

    val tableRef= new TableReference
    tableRef.setProjectId(projectId)
    tableRef.setDatasetId(dataSetId)
    tableRef.setTableId(tableName)
    table.setTableReference(tableRef)

    try{
      val response = bigquery.tables.insert(projectId, dataSetId, table).execute()
      println("Table created")
    }catch{
      case ex: IOException =>
        println("IOException while creating table: " + ex)
    }
  }

  /**
   * Delete the testtable table
   */
  def deleteTable(projectId: String, bigquery: Bigquery) = {

    val dataSetId = "testdataset"

    val tableName = "testtable"

    try{
      bigquery.tables.delete(projectId, dataSetId, tableName).execute()
      println("Table deleted")
    }catch{
      case ex: IOException =>
        println("IOException while deleting table: " + ex)
    }
  }

  /**
   * Adds a row to the table.
   * Uses a syncronous request.
   */
  def addRow(projectId: String, bigquery: Bigquery) = {
    
    val dataSetId = "testdataset"

    val tableName = "testtable"

    val tableRow = new TableRow
    tableRow.set("firstName", "abc")
    tableRow.set("lastName", "def")
    tableRow.set("age", 123)

    val rows = new TableDataInsertAllRequest.Rows
    rows.setJson(tableRow);
    println(rows.getJson)

    val rowList = new ArrayList[TableDataInsertAllRequest.Rows]
    rowList.add(rows)
    
    val tableData = new TableDataInsertAllRequest
    tableData.setRows(rowList)
    
    // try{
      val response = bigquery.tabledata.insertAll(projectId, dataSetId, tableName, tableData).execute()

    println("response: "+response)
    //  println("Row added")
    //   println("Response: "+response)
   // }catch{
    //  case ex: IOException =>
     //   println("IOException while adding row: " + ex)
    //}
  }

  /**
   * NOT WORKING
   * Adds data from a local json file.
   * @see [[https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll the official google docs]]
   * for the specification of the request body.
   * Uses an asyncronous request.
   */
  def addRowsFromFile(projectId: String, bigquery: Bigquery) = {

    val dataSetId = "testdataset"
    
    val tableName = "testtable"

    val destTable = new TableReference
    destTable.setProjectId(projectId)
    destTable.setDatasetId(dataSetId)
    destTable.setTableId(tableName)

    val job = new Job
    val config = new JobConfiguration
    val configLoad = new JobConfigurationLoad
    configLoad.setDestinationTable(destTable);
    configLoad.setEncoding("UTF-8");
    configLoad.setWriteDisposition("WRITE_APPEND")
    config.setLoad(configLoad)
    job.setConfiguration(config)

    val content = new FileContent("application/octet-stream", new File("data.json"))
    println(content)

    val request = bigquery.jobs.insert(projectId, job, content)
    println("add rows from file request: " + request.getJsonContent)

    val response = request.execute()
    println("add rows from file response: " + response)
  }
  
  /**
   * Prompts the user to visit the google API authorization page. The user 
   * can then grant access to the API, and if so is given an access code. The 
   * user is prompted to paste this code in to the command line. The code grants
   * the applicatin access to the database.
   *
   * @return valid credentials
   */
  def getCredentials: Credential = {

    val fis = new FileInputStream(ClientSecretsLocation)
    val reader = new InputStreamReader(fis)
    val clientSecrets = GoogleClientSecrets.load(new JacksonFactory, reader)

    val scopes = Collections.singleton(BigqueryScopes.BIGQUERY)
    val authorizeUrl = new GoogleAuthorizationCodeRequestUrl(clientSecrets, clientSecrets.getInstalled().getRedirectUris().get(0), scopes).build()
    println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl)
    println("... and paste the code you received here: ")
    val authorizationCode = readLine()


    // Exchange the auth code for an access token
    val flow = new GoogleAuthorizationCodeFlow.Builder(HttpTransport, JsonFactory, clientSecrets, Arrays.asList(BigqueryScopes.BIGQUERY)).build()
    val response = flow.newTokenRequest(authorizationCode).setRedirectUri(clientSecrets.getInstalled.getRedirectUris.get(0)).execute();
    flow.createAndStoreCredential(response,null)
  }

  /**
   * Makes a request to the database and displays the response.
   *
   * @param bigquery
   * @param projectId
   * @param query [[https://cloud.google.com/bigquery/query-reference bigquery database query]]
   * @param out
   */
  def runQueryRpcAndPrint(bigquery: Bigquery, projectId: String, query: String, out: PrintStream) {
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
   * Store the refresh token in the file 
   * snowplow_bigquery_refresh_token.properties
   */
  def storeRefreshToken(refreshToken: String){
    val properties = new Properties
    properties.setProperty("refreshtoken", refreshToken)
    try {
      properties.store(new FileOutputStream("snowplow_bigquery_refresh_token.properties"), null)
      println("Refresh token saved.")
    } catch {
        case ex: FileNotFoundException => 
          println("FileNotFoundException: " + ex)
        case ex: IOException => 
          println("IOException: " + ex)
    }
  }
  
  /**
   * Load the refresh token from the file 
   * snowplow_bigquery_refresh_token.properties
   */
  def loadRefreshToken: Option[String] = {
    val properties = new Properties
    try {
      properties.load(new FileInputStream("snowplow_bigquery_refresh_token.properties"))
      Some(properties.getProperty("refreshtoken"))
    } catch {
        case ex: FileNotFoundException => {
          println("FileNotFoundException: " + ex)
          None
        }
        case ex: IOException => {
          println("IOException: " + ex)
          None
        }
    }
  }

  /**
   * Get access token from refresh token.
   */
  def createCredentialWithRefreshToken(transport: HttpTransport, jsonFactory: JsonFactory, tokenResponse: TokenResponse): GoogleCredential = {

    val fis = new FileInputStream(ClientSecretsLocation)
    val reader = new InputStreamReader(fis)
    val clientSecrets = GoogleClientSecrets.load(new JacksonFactory, reader)

    new GoogleCredential.Builder().setTransport(transport)
      .setJsonFactory(jsonFactory)
      .setClientSecrets(clientSecrets)
      .build()
      .setFromTokenResponse(tokenResponse)
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
