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
package com.snowplowanalytics.snowplow.collectors
package scalastream

// Scala
import scala.collection.mutable.MutableList

// Akka
import akka.actor.{ActorSystem, Props}

// Specs2 and Spray testing
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.specification.{Scope,Fragments}
import spray.testkit.Specs2RouteTest

// Spray
import spray.http.{DateTime,HttpHeader,HttpRequest,HttpCookie}
import spray.http.HttpHeaders.{
  Cookie,
  `Set-Cookie`,
  `Remote-Address`,
  `Raw-Request-URI`
}

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Thrift
import org.apache.thrift.TDeserializer

// Snowplow
import sinks._
import thrift.{
  PayloadProtocol,
  PayloadFormat,
  SnowplowRawEvent
}

class CollectorServiceSpec extends Specification with Specs2RouteTest with
     AnyMatchers {
   val testConf: Config = ConfigFactory.parseString("""
collector {
  interface = "0.0.0.0"
  port = 8080

  production = true

  p3p {
    policyref = "/w3c/p3p.xml"
    CP = "NOI DSP COR NID PSA OUR IND COM NAV STA"
  }

  cookie {
    expiration = 365 days
    domain = "test-domain.com"
  }

  sink {
    enabled = "test"

    kinesis {
      aws {
        access-key: "cpf"
        secret-key: "cpf"
      }
      stream {
        name: "snowplow_collector_example"
        size: 1
      }
    }
  }
}
""")
  val collectorConfig = new CollectorConfig(testConf)
  val sink = new TestSink
  val responseHandler = new ResponseHandler(collectorConfig, sink)
  val collectorService = new CollectorService(responseHandler, system)
  val thriftDeserializer = new TDeserializer

  // By default, spray will always add Remote-Address to every request
  // when running with the `spray.can.server.remote-address-header`
  // option. However, the testing does not read this option and a
  // remote address always needs to be set.
  def CollectorGet(uri: String, cookie: Option[`HttpCookie`] = None,
      remoteAddr: String = "127.0.0.1") = {
    val headers: MutableList[HttpHeader] =
      MutableList(`Remote-Address`(remoteAddr),`Raw-Request-URI`(uri))
    if (cookie.isDefined) headers += `Cookie`(cookie.get)
    Get(uri).withHeaders(headers.toList)
  }

  "Snowplow's Scala collector" should {
    "return an invisible pixel" in {
      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
        responseAs[Array[Byte]] === ResponseHandler.pixel
      }
    }
    "return a cookie expiring at the correct time" in {
      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
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
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration -
          DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 2000) // 1000 ms window.
      }
    }
    "return the same cookie as passed in" in {
      CollectorGet("/i", Some(HttpCookie("sp", "UUID_Test"))) ~>
          collectorService.collectorRoute ~> check {
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
    "return a P3P header" in {
      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
        val p3pHeaders = headers.filter {
          h => h.name.equals("P3P")
        }
        p3pHeaders.size must beEqualTo(1)
        val p3pHeader = p3pHeaders(0)

        val policyRef = collectorConfig.p3pPolicyRef
        val CP = collectorConfig.p3pCP
        p3pHeader.value must beEqualTo(
          s"""policyref="${policyRef}", CP="${CP}"""")
      }
    }
    "store the expected event as a serialized Thrift object in the enabled sink" in {
      val payloadData = "param1=val1&param2=val2"
      val storedRecordBytes = responseHandler.cookie(payloadData, None,
        None, "localhost", "127.0.0.1", new HttpRequest(), None)._2

      val storedEvent = new SnowplowRawEvent
      this.synchronized {
        thriftDeserializer.deserialize(storedEvent, storedRecordBytes)
      }

      storedEvent.timestamp must beCloseTo(DateTime.now.clicks, 1000)
      storedEvent.encoding must beEqualTo("UTF-8")
      storedEvent.ipAddress must beEqualTo("127.0.0.1")
      storedEvent.collector must beEqualTo("ssc-0.1.0-test")
      storedEvent.payload.protocol must beEqualTo(PayloadProtocol.Http)
      storedEvent.payload.format must beEqualTo(PayloadFormat.HttpGet)
      storedEvent.payload.data must beEqualTo(payloadData)
    }
  }
}
