/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream

// Scala
import org.specs2.mock.Mockito
import spray.http.{Rendering, StatusCodes}

import scala.collection.mutable.MutableList

// Akka
import akka.actor.{ActorSystem, Props}

// Specs2 and Spray testing
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest

// Spray
import spray.http.{DateTime,HttpHeader,HttpRequest,HttpCookie,RemoteAddress}
import spray.http.HttpHeaders._

// Config
import com.typesafe.config.{ConfigFactory,Config}

// Thrift
import org.apache.thrift.TDeserializer

// Snowplow
import sinks._
import CollectorPayload.thrift.model1.CollectorPayload

class CollectorServiceSpec extends Specification with Specs2RouteTest with Mockito with
     AnyMatchers {


   def testConf(n3pcEnabled: Boolean): Config = ConfigFactory.parseString(s"""
collector {
  interface = "0.0.0.0"
  port = 8080

  production = true
  third-party-redirect-enabled = $n3pcEnabled
  p3p {
    policyref = "/w3c/p3p.xml"
    CP = "NOI DSP COR NID PSA OUR IND COM NAV STA"
  }

  cookie {
    enabled = true
    expiration = 365 days
    name = sp
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
        region: "us-east-1"
        good: "snowplow_collector_example"
        bad: "snowplow_collector_example"
      }
      buffer {
        byte-limit: 4000000 # 4MB
        record-limit: 500 # 500 records
        time-limit: 60000 # 1 minute
      }
      backoffPolicy {
        minBackoff: 3000 # 3 seconds
        maxBackoff: 600000 # 5 minutes
      }
    }
  }
}
""")

  "Snowplow's Scala collector with n3pc redirect " should {
    val collectorConfig = new CollectorConfig(testConf(n3pcEnabled = true))

    // By default, spray will always add Remote-Address to every request
    // when running with the `spray.can.server.remote-address-header`
    // option. However, the testing does not read this option and a
    // remote address always needs to be set.
    def CollectorGet(uri: String, cookie: Option[`HttpCookie`] = None,
                     remoteAddr: String = "127.0.0.1", additionalHeaders: List[HttpHeader] = List()) = {
      val headers: MutableList[HttpHeader] =
        MutableList(`Remote-Address`(remoteAddr),`Raw-Request-URI`(uri)) ++ additionalHeaders
      cookie.foreach(headers += `Cookie`(_))
      Get(uri).withHeaders(headers.toList)
    }
    "return a redirect with n3pc parameter while keeping request params and using original site schema" in {
      val sink = smartMock[AbstractSink]
      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
        case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
      }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      val originalUrl = "/i?uid=someUid&cx=someCx&url=https://example.com/something"
      CollectorGet(originalUrl) ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.Found)
        //we're redirecting so we should not save anything to kinesis
        there was no(sink).storeRawEvents(any, any)
        val locationHeader: String = header("Location").get.value
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        locationHeader must beEqualTo(s"https://example.com$originalUrl&${collectorConfig.thirdPartyCookiesParameter}=true")
        locationHeader must contain(collectorConfig.thirdPartyCookiesParameter)
        //a cookie must be sent back
        httpCookies must not be empty
      }
    }

    "return a redirect with n3pc parameter with protocol from X-Forwarded-Proto" in {
      val sink = smartMock[AbstractSink]

      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
        case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
      }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      val originalUrl = "/i?uid=someUid&cx=someCx"
      CollectorGet(originalUrl, additionalHeaders = List(RawHeader("X-Forwarded-Proto", "https"))) ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.Found)
        //we're redirecting so we should not save anything to kinesis
        there was no(sink).storeRawEvents(any, any)
        val locationHeader: String = header("Location").get.value
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        locationHeader must beEqualTo(s"https://example.com$originalUrl&${collectorConfig.thirdPartyCookiesParameter}=true")
        locationHeader must contain(collectorConfig.thirdPartyCookiesParameter)
        //a cookie must be sent back
        httpCookies must not be empty
      }
    }

    "return a redirect with n3pc parameter while keeping request params" in {
      val sink = smartMock[AbstractSink]
      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
        case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
      }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      val originalUrl = "/i?uid=someUid&cx=someCx"
      CollectorGet(originalUrl) ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.Found)
        //we're redirecting so we should not save anything to kinesis
        there was no(sink).storeRawEvents(any, any)
        val locationHeader: String = header("Location").get.value
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        locationHeader must beEqualTo(s"http://example.com$originalUrl&${collectorConfig.thirdPartyCookiesParameter}=true")
        locationHeader must contain(collectorConfig.thirdPartyCookiesParameter)
        //a cookie must be sent back
        httpCookies must not be empty
      }
    }

    "return a redirect with n3pc parameter " in {
      val sink = smartMock[AbstractSink]
      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
          case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
        }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)

      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.Found)
        there was no(sink).storeRawEvents(any, any)
        val locationHeader: String = header("Location").get.value
        locationHeader must contain(collectorConfig.thirdPartyCookiesParameter)
      }
    }

    "set the fallback cookie value after calling it with n3pc parameter without cookies" in {
      val sink = smartMock[AbstractSink]
      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
          case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
        }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      CollectorGet(s"/i?${collectorConfig.thirdPartyCookiesParameter}") ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.OK)
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        there was two(sink).storeRawEvents(any, any)
        httpCookies must not be empty
        val httpCookie = httpCookies.head
        httpCookie.content mustEqual collectorConfig.fallbackNetworkUserId
      }
    }

    "return the correct cookie even after calling it with n3pc parameter" in {
      val sink = smartMock[AbstractSink]
      sink.storeRawEvents(any[List[Array[Byte]]], anyString).answers{(params, mock) => params match {
          case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
        }
      }
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)

      CollectorGet(s"/i?${collectorConfig.thirdPartyCookiesParameter}", Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~>
        collectorService.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        there was two(sink).storeRawEvents(any, any)
        val httpCookie = httpCookies.head
        httpCookie.content must beEqualTo("UUID_Test")
      }
    }

    "return an invisible pixel" in {
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      CollectorGet(s"/i?${collectorConfig.thirdPartyCookiesParameter}") ~> collectorService.collectorRoute ~> check {
        responseAs[Array[Byte]] === ResponseHandler.pixel
      }
    }

    "return a cookie expiring at the correct time" in {
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
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

        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get -
          DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 3600000) // 1 hour window.
      }
    }
    "return the same cookie as passed in" in {
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      CollectorGet("/i", Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~>
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
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
        val p3pHeaders = headers.filter {
          h => h.name.equals("P3P")
        }
        p3pHeaders.size must beEqualTo(1)
        val p3pHeader = p3pHeaders(0)

        val policyRef = collectorConfig.p3pPolicyRef
        val CP = collectorConfig.p3pCP
        p3pHeader.value must beEqualTo(
          "policyref=\"%s\", CP=\"%s\"".format(policyRef, CP))
      }
    }

    "do not store the expected event if there's no cookie" in {
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)

      val payloadData = "param1=val1&param2=val2"
      val storedRecordBytes = responseHandler.cookie(payloadData, null, None,
        None, "localhost", RemoteAddress("127.0.0.1"), new HttpRequest(), None, "/i", pixelExpected = true)._2
      storedRecordBytes must beEmpty
    }

    "store the expected event as a serialized Thrift object in the enabled sink only if cookie is present" in {
      val payloadData = "param1=val1&param2=val2"
      val sink = new TestSink
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val thriftDeserializer = new TDeserializer
      val cookie = HttpCookie("SomeName",  "SOME_UUID")
      val storedRecordBytes = responseHandler.cookie(payloadData, null, Some(cookie),
        None, "localhost", RemoteAddress("127.0.0.1"), new HttpRequest(), None, s"/i", pixelExpected = true)._2

      val storedEvent = new CollectorPayload
      this.synchronized {
        thriftDeserializer.deserialize(storedEvent, storedRecordBytes.head)
      }

      storedEvent.timestamp must beCloseTo(DateTime.now.clicks, 60000)
      storedEvent.encoding must beEqualTo("UTF-8")
      storedEvent.ipAddress must beEqualTo("127.0.0.1")
      storedEvent.collector must beEqualTo("ssc-0.7.0-test")
      storedEvent.path must beEqualTo("/i")
      storedEvent.querystring must beEqualTo(payloadData)
    }

    "report itself as healthy" in {
      val sink = smartMock[AbstractSink]
      val sinks = CollectorSinks(sink, sink)
      val responseHandler = new ResponseHandler(collectorConfig, sinks)
      val collectorService = new CollectorService(collectorConfig, responseHandler, system)
      CollectorGet("/health") ~> collectorService.collectorRoute ~> check {
        response.status must beEqualTo(spray.http.StatusCodes.OK)
      }
    }
  }

  "Snowplow's Scala collector with n3pc redirect disabled" should {
    val collectorConfig = new CollectorConfig(testConf(n3pcEnabled = false))

    // By default, spray will always add Remote-Address to every request
    // when running with the `spray.can.server.remote-address-header`
    // option. However, the testing does not read this option and a
    // remote address always needs to be set.
    def CollectorGet(uri: String, cookie: Option[`HttpCookie`] = None,
                     remoteAddr: String = "127.0.0.1") = {
      val headers: MutableList[HttpHeader] =
        MutableList(`Remote-Address`(remoteAddr),`Raw-Request-URI`(uri))
      cookie.foreach(headers += `Cookie`(_))
      Get(uri).withHeaders(headers.toList)
    }
    val sink = new TestSink
    val sinks = CollectorSinks(sink, sink)
    val responseHandler = new ResponseHandler(collectorConfig, sinks)
    val collectorService = new CollectorService(collectorConfig, responseHandler, system)
    val thriftDeserializer = new TDeserializer
    "return an invisible pixel" in {
      CollectorGet("/i") ~> collectorService.collectorRoute ~> check {
        response.status.mustEqual(StatusCodes.OK)
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

        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get -
          DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 3600000) // 1 hour window.
      }
    }
    "return the same cookie as passed in" in {
      CollectorGet("/i", Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~>
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
          "policyref=\"%s\", CP=\"%s\"".format(policyRef, CP))
      }
    }
    "store the expected event as a serialized Thrift object in the enabled sink" in {
      val payloadData = "param1=val1&param2=val2"
      val storedRecordBytes = responseHandler.cookie(payloadData, null, None,
        None, "localhost", RemoteAddress("127.0.0.1"), new HttpRequest(), None, "/i", true)._2

      val storedEvent = new CollectorPayload
      this.synchronized {
        thriftDeserializer.deserialize(storedEvent, storedRecordBytes.head)
      }

      storedEvent.timestamp must beCloseTo(DateTime.now.clicks, 60000)
      storedEvent.encoding must beEqualTo("UTF-8")
      storedEvent.ipAddress must beEqualTo("127.0.0.1")
      storedEvent.collector must beEqualTo("ssc-0.7.0-test")
      storedEvent.path must beEqualTo("/i")
      storedEvent.querystring must beEqualTo(payloadData)
    }
  }
}
