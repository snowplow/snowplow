/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics.scalacollector

import org.apache.commons.codec.binary.Base64
import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import spray.http.HttpHeaders.`Set-Cookie`
import StatusCodes._

// http://spray.io/documentation/1.2.0/spray-testkit/
class CollectorServiceSpec extends Specification with Specs2RouteTest with
     CollectorService {
   def actorRefFactory = system

  "CollectorService service" should {
    "return an invisible pixel." in {
      Get("/i") ~> route ~> check {
        responseAs[Array[Byte]] === Base64.decodeBase64("R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")
      }
    }
    "return a cookie expiring in a year." in {
      Get("/i") ~> route ~> check {
        (headers contains `Set-Cookie`) && false
        // TODO: Expires in a year
      }
    }
    "return the same cookie as passed in." in {
      Get("/i") ~> route ~> check {
        false // TODO
      }
    }
  }
}
