/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

// Java
import java.io.FileNotFoundException

// Scala
import collection.JavaConversions._

// Google
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

import com.google.api.services.bigquery.model.{
  TableDataInsertAllResponse,
  ErrorProto
}
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors
// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

object BigqueryInterfaceSpec {

  /**
   * At the moment the same as the method in TsvParser.
   */
  def makeTestRequest = TsvParser.createUploadData(_)

  /**
   * Creates responses for testing.
   *
   * @param errorList List of strings representing the errors for each row.
   *
   * @returns TableDataInsertAllResponse
   */
  def makeTestResponse(errorList: List[String]): TableDataInsertAllResponse = {
    val response = new TableDataInsertAllResponse
    val errorProtoList = errorList.map(error => {
      (new ErrorProto).setReason(error)
    })
    if (errorProtoList.isEmpty){
      response
    } else {
      val rowList = new TableDataInsertAllResponse.InsertErrors()
      rowList.setErrors(errorProtoList)
      response.setInsertErrors(List(rowList))
    }
  }

}

class BigqueryInterfaceSpec extends Specification with ValidationMatchers {

  val configFile = "src/test/resources/application.conf.test" 
  val privatekeyFile = "src/test/resources/pk_test.p12"
  val config = SnowplowBigqueryEmitter.getConfigFromFile( configFile )

  val configFileWrongP12 = "src/test/resources/application.conf.wrongP12"
  val configWrongP12 = SnowplowBigqueryEmitter.getConfigFromFile( configFileWrongP12 )

  val abstractRows = List(
    List(("firstname", "STRING", "Terry"), ("lastname", "STRING", "Test"), ("age", "INTEGER", "22")),
    List(("firstname", "STRING", "Tony"), ("lastname", "STRING", "Test"), ("age", "INTEGER", "34")),  
    List(("firstname", "STRING", "Tonyah"), ("lastname", "STRING", "Test"), ("age", "INTEGER", "56"))  
  )
  
  "getCredentials" should {
    "return a GoogleCredential object" in {
      BigqueryInterface.getCredentials( config ) should haveClass[GoogleCredential] 
    }
    "throw an exception - with friendly message - if private key file not found" in {
      BigqueryInterface.getCredentials( configWrongP12 ) should 
        throwA[FileNotFoundException](message=
          "Private key file not found at location specified at 'service-account-p12file:' in config file."
        )
    }
  }

  "checkResponseHasErrors" should {

    "return true for response with errors" in {
      val errorResponse = BigqueryInterfaceSpec.makeTestResponse(List("stopped", "invalid", "invalid"))
      BigqueryInterface.checkResponseHasErrors(errorResponse) should beTrue
    }
    "return false for response with no errors" in {
      val goodResponse = BigqueryInterfaceSpec.makeTestResponse(List())
      BigqueryInterface.checkResponseHasErrors(goodResponse) should beFalse
    }
  }

  "seperateRows" should {
    """for a request with all good rows, return object with 
    --- first element of pair an empty list""" in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val goodResponse = BigqueryInterfaceSpec.makeTestResponse(List())
      val seperated = BigqueryInterface.seperateRows(request, goodResponse)
      seperated._1.isEmpty should beTrue
    }
    " --- second element of pair of same length as abstractRows" in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val goodResponse = BigqueryInterfaceSpec.makeTestResponse(List())
      val seperated = BigqueryInterface.seperateRows(request, goodResponse)
      seperated._2.length should beEqualTo(abstractRows.length)
    }
    """for a request with two bad rows, return object with
    --- first element of length 2""" in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val errorResponse = BigqueryInterfaceSpec
        .makeTestResponse(List("stopped", "invalid", "invalid"))
      val seperated = BigqueryInterface.seperateRows(request, errorResponse)
      seperated._1.length should beEqualTo(2)
    }
    " --- second element of length of abstractRow minus 2 " in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val errorResponse = BigqueryInterfaceSpec
        .makeTestResponse(List("stopped", "invalid", "invalid"))
      val seperated = BigqueryInterface.seperateRows(request, errorResponse)
      seperated._2.length should beEqualTo(abstractRows.length-2)
    }

    "throw a RuntimeException if errorList contains unexpected errors" in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val errorResponse = BigqueryInterfaceSpec
        .makeTestResponse(List("stopped", "invalid", "foo"))
      BigqueryInterface.seperateRows(request, errorResponse) should 
        throwA[RuntimeException](message=
            "The error message: foo" + 
            " is unexpected. This is a developer error - create an issue."
          )
    }

    "request and response with bad rows should throw RuntimeException if length of" +
    " lists of rows and errors are not equal" in {
      val request = BigqueryInterfaceSpec.makeTestRequest(abstractRows)
      val errorResponse = BigqueryInterfaceSpec
        .makeTestResponse(List("stopped", "invalid"))
      BigqueryInterface.seperateRows(request, errorResponse) should 
        throwA[RuntimeException](message=
          "List of rows in request is not of same length as list of errors in response. "
        + "Perhaps the request and response are not related?"
        )
    }

  }
    
}
