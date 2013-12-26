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

// specs2 and spray testing libraries.
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import spray.testkit.Specs2RouteTest

// Spray classes.
import spray.http.{DateTime,HttpHeader,HttpCookie}
import spray.http.HttpHeaders.{Cookie,`Set-Cookie`,`Remote-Address`}

import scala.collection.mutable.MutableList

// http://spray.io/documentation/1.2.0/spray-testkit/
class CollectorServiceSpec extends Specification with Specs2RouteTest with
     CollectorService with AnyMatchers {
  def actorRefFactory = system

  // By default, spray will always add Remote-Address to every request
  // when running with the `spray.can.server.remote-address-header`
  // option. However, the testing does not read this option and a
  // remote address always needs to be set.
  def CollectorGet(uri: String, cookie: Option[`HttpCookie`] = None,
      remoteAddr: String = "127.0.0.1") = {
    val headers: MutableList[HttpHeader] =
      MutableList(`Remote-Address`(remoteAddr))
    if (cookie.isDefined) headers += `Cookie`(cookie.get)
    Get(uri).withHeaders(headers.toList)
  }

  "Snowplow's Scala collector" should {
    "return an invisible pixel." in new InitKinesis {
      CollectorGet("/i") ~> collectorRoute ~> check {
          responseAs[Array[Byte]] === Responses.pixel
        }
    }
    "return a cookie expiring at the correct time." in new InitKinesis {
      CollectorGet("/i") ~> collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.name must be("sp")
        httpCookie.expires must beSome
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - CollectorConfig.cookieExpiration -
          DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 2000) // 1000 ms window.
      }
    }
    "return the same cookie as passed in." in new InitKinesis {
      CollectorGet("/i", Some(HttpCookie("sp", "UUID_Test"))) ~>
          collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.content must beEqualTo("UUID_Test")
      }
    }
    "store multiple requests in Kinesis." in new InitKinesis {
      // TODO
    }
  }
}

trait InitKinesis extends Scope {
  // TODO: Clear the Kinesis stream.
  if (!KinesisInterface.createStream()) {
    throw new RuntimeException("Unable to initialize Kinesis stream.")
  }
}
