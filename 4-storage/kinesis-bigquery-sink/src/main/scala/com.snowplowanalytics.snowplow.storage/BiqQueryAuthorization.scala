package com.google.cloud.helix.samples // TODO: change package and where this file lives on disk. see Fred's ES Sink for package naming conventions and folder layout

// TODO: remove all ;s
// TODO: group all imports, like java.io below. note that you can only group things at the same level
// TODO: move these imports below the Scala group. We import in this order: Java, Scala, then Java libraries, then Scala libraries
//
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

// TODO: add a description. see other snowplow projects for examples
object bigQueryAuth {

  val ProjectId = "742196692985"
  val ClientSecretsLocation = "client_secrets.json"
  val HttpTransport = new NetHttpTransport
  val JsonFactory = new JacksonFactory

  def main(args: Array[String]) {
   
    val credentials = {
      val fis = new FileInputStream(ClientSecretsLocation)
      val reader = new InputStreamReader(fis)
      val gcs = GoogleClientSecrets.load(new JacksonFactory, reader)
      getCredentials(gcs)
    }
	 
    val bigquery = new Bigquery(HttpTransport, JsonFactory, credentials);
    // TODO: indent below's last 2 lines
    val query = "SELECT TOP( title, 10) as title, COUNT(*) as revision_count " +
		"FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;"
    runQueryRpcAndPrint(bigquery, ProjectId, query, System.out);
  }

  // TODO: add description
  def getCredentials(clientSecrets: GoogleClientSecrets): Credential = {

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

  // TODO: add description
  def runQueryRpcAndPrint(bigquery: Bigquery, projectId: String, query: String, out: PrintStream) {
    val queryRequest = new QueryRequest().setQuery(query)
    val queryResponse = bigquery.jobs.query(projectId, queryRequest).execute()
    
    if (queryResponse.getJobComplete) {
      printRows(queryResponse.getRows, out)
      if (Option(queryResponse.getPageToken).isDefined) {
        return
      }
    }

    while(true){
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
  
  def printRows(rows: List[TableRow], out: PrintStream){

    def outputCell(cell: TableCell){
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
