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

// Google
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

class BigqueryInterfaceSpec extends Specification with ValidationMatchers {

  val configFile = "src/test/resources/application.conf.test" 
  val privatekeyFile = "src/test/resources/pk_test.p12"
  val config = SnowplowBigqueryEmitter.getConfigFromFile( configFile )

  val configFileWrongP12 = "src/test/resources/application.conf.wrongP12"
  val configWrongP12 = SnowplowBigqueryEmitter.getConfigFromFile( configFileWrongP12 )
  
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
}
