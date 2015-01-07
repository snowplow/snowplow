package com.google.cloud.helix.samples

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

// Java
import java.io.{
  FileInputStream,
  IOException,
  InputStreamReader,
  PrintStream
}
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

// Scala
import collection.JavaConversions._

object bigQueryAuth {
  val ProjectId = "742196692985"
  val ClientSecretsLocation = "client_secrets.json"
  val HttpTransport = new NetHttpTransport
  val JsonFactory = new JacksonFactory
  def main(args: Array[String]) {
    val fileInputStream = new FileInputStream(ClientSecretsLocation)
    val reader = new InputStreamReader(fileInputStream)
    val clientSecrets = GoogleClientSecrets.load(new JacksonFactory, reader)
    val credential = getCredentials(clientSecrets)
	  val bigquery = new Bigquery(HttpTransport, JsonFactory, credential);
    val query = "SELECT TOP( title, 10) as title, COUNT(*) as revision_count " +
		"FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;"
    runQueryRpcAndPrint(bigquery, ProjectId, query, System.out);
  }

  def getCredentials(clientSecrets: GoogleClientSecrets): Credential = {

    val scopes = Collections.singleton(BigqueryScopes.BIGQUERY)
    val authorizeUrl = new GoogleAuthorizationCodeRequestUrl(clientSecrets, clientSecrets.getInstalled().getRedirectUris().get(0), scopes).build()
    println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl)
    println("... and paste the code you received here: ")
    val authorizationCode = readLine()

    // Exchange the auth code for an access token.
    val flow = new GoogleAuthorizationCodeFlow.Builder(HttpTransport, JsonFactory, clientSecrets, Arrays.asList(BigqueryScopes.BIGQUERY)).build()
    val response = flow.newTokenRequest(authorizationCode).setRedirectUri(clientSecrets.getInstalled().getRedirectUris().get(0)).execute();
    flow.createAndStoreCredential(response,null)
  }

  def runQueryRpcAndPrint(bigquery: Bigquery, projectId: String, query: String, out: PrintStream){
    val queryRequest = new QueryRequest().setQuery(query)
    val queryResponse = bigquery.jobs().query(projectId, queryRequest).execute()
    var pageToken:String = null
    if (queryResponse.getJobComplete()){
      printRows(queryResponse.getRows(), out)
      if (pageToken == null){
        return
      }
    }
    while(true){
      val queryResults = bigquery.jobs()
        .getQueryResults(projectId, queryResponse.getJobReference().getJobId())
        .setPageToken(pageToken).execute()
      if (queryResults.getJobComplete()){
        printRows(queryResponse.getRows, out)
        pageToken = queryResults.getPageToken()
        if (pageToken == null){
          return
        }
      }
    }
  }
  
  def printRows(rows: List[TableRow], out: PrintStream){

    def outputCell(cell: TableCell){
      val cell_data = if (Data.isNull(cell.getV())){
        "Null"
      }else{
        cell.getV().toString()
      }
      out.printf("%s, ", cell_data)
    }
    if (rows != null){
      for (row <- rows){
        for (cell <- row.getF()){
          outputCell(cell)
        }
        out.println()
      }
    }
  }
  
}
