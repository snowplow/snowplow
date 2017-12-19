/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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
package collectors.scalastream

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import org.apache.thrift.{TSerializer, TDeserializer}
import org.specs2.mutable.Specification

import CollectorPayload.thrift.model1.CollectorPayload
import generated.Settings
import model._

class CollectorServiceSpec extends Specification {
  val service = new CollectorService(
    TestUtils.testConf,
    CollectorSinks(new TestSink, new TestSink)
  )
  val bouncingService = new CollectorService(
    TestUtils.testConf.copy(cookieBounce = TestUtils.testConf.cookieBounce.copy(enabled = true)),
    CollectorSinks(new TestSink, new TestSink)
  )
  val uuidRegex = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".r
  val event = new CollectorPayload(
    "iglu-schema", "ip", System.currentTimeMillis, "UTF-8", "collector")
  val hs = List(`Raw-Request-URI`("uri"))
  val serializer = new TSerializer()
  val deserializer = new TDeserializer()

  "The collector service" should {
    "cookie" in {
      "attach p3p headers" in {
        val (r, l) = service.cookie(Some("nuid=12"), Some("b"), "p", None, None, None, "h",
          RemoteAddress.Unknown, HttpRequest(), false)
        r.headers must have size 4
        r.headers must contain(RawHeader("P3P", "policyref=\"%s\", CP=\"%s\""
            .format("/w3c/p3p.xml", "NOI DSP COR NID PSA OUR IND COM NAV STA")))
        r.headers must contain(`Access-Control-Allow-Origin`(HttpOriginRange.`*`))
        r.headers must contain(`Access-Control-Allow-Credentials`(true))
        l must have size 1
      }
      "not store stuff if bouncing and provide a location header" in {
        val (r, l) = bouncingService.cookie(
          None, Some("b"), "p", None, None, None, "h", RemoteAddress.Unknown, HttpRequest(), true)
        r.headers must have size 5
        r.headers must contain(`Location`("/?bounce=true"))
        l must have size 0
      }
      "store stuff if having already bounced with the fallback nuid" in {
        val (r, l) = bouncingService.cookie(Some("bounce=true"), Some("b"), "p", None, None, None,
          "h", RemoteAddress.Unknown, HttpRequest(), true)
        r.headers must have size 4
        l must have size 1
        val newEvent = new CollectorPayload(
          "iglu-schema", "ip", System.currentTimeMillis, "UTF-8", "collector")
        deserializer.deserialize(newEvent, l.head)
        newEvent.networkUserId shouldEqual "new-nuid"
      }
    }

    "preflightResponse" in {
      "return a response appropriate to cors preflight options requests" in {
        service.preflightResponse(HttpRequest()) shouldEqual HttpResponse()
          .withHeaders(List(
            `Access-Control-Allow-Origin`(HttpOriginRange.`*`),
            `Access-Control-Allow-Credentials`(true),
            `Access-Control-Allow-Headers`("Content-Type")
          ))
      }
    }

    "flashCrossDomainPolicy" in {
      "return the cross domain policy" in {
        service.flashCrossDomainPolicy shouldEqual HttpResponse(
          entity = HttpEntity(
            contentType = ContentType(MediaTypes.`text/xml`, HttpCharsets.`ISO-8859-1`),
            string = "<?xml version=\"1.0\"?>\n<cross-domain-policy>\n  <allow-access-from domain=\"*\" secure=\"false\" />\n</cross-domain-policy>"
          )
        )
      }
    }

    "buildEvent" in {
      "fill the correct values" in {
        val l = `Location`("l")
        val ct = Some("image/gif")
        val r = HttpRequest().withHeaders(l :: hs)
        val e = service
          .buildEvent(Some("q"), Some("b"), "p", Some("ua"), Some("ref"), "h", "ip", r, "nuid", ct)
        e.schema shouldEqual "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
        e.ipAddress shouldEqual "ip"
        e.encoding shouldEqual "UTF-8"
        e.collector shouldEqual s"${Settings.shortName}-${Settings.version}-kinesis"
        e.querystring shouldEqual "q"
        e.body shouldEqual "b"
        e.path shouldEqual "p"
        e.userAgent shouldEqual "ua"
        e.refererUri shouldEqual "ref"
        e.hostname shouldEqual "h"
        e.networkUserId shouldEqual "nuid"
        e.headers shouldEqual (List(l) ++ ct).map(_.toString).asJava
        e.contentType shouldEqual ct.get
      }
      "have a null queryString if it's None" in {
        val l = `Location`("l")
        val ct = Some("image/gif")
        val r = HttpRequest().withHeaders(l :: hs)
        val e = service
          .buildEvent(None, Some("b"), "p", Some("ua"), Some("ref"), "h", "ip", r, "nuid", ct)
        e.schema shouldEqual "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
        e.ipAddress shouldEqual "ip"
        e.encoding shouldEqual "UTF-8"
        e.collector shouldEqual s"${Settings.shortName}-${Settings.version}-kinesis"
        e.querystring shouldEqual null
        e.body shouldEqual "b"
        e.path shouldEqual "p"
        e.userAgent shouldEqual "ua"
        e.refererUri shouldEqual "ref"
        e.hostname shouldEqual "h"
        e.networkUserId shouldEqual "nuid"
        e.headers shouldEqual (List(l) ++ ct).map(_.toString).asJava
        e.contentType shouldEqual ct.get
      }
    }

    "sinkEvent" in {
      "send back the produced events" in {
        val l = service.sinkEvent(event, "key")
        l must have size 1
        l.head.zip(serializer.serialize(event)).forall { case (a, b) => a mustEqual b }
      }
    }

    "buildHttpResponse" in {
      val conf = TestUtils.testConf.streams.sink
      "rely on buildRedirectHttpResponse if redirect is true" in {
        val (res, Nil) =
          service.buildHttpResponse(event, "k", Map("u" -> "12"), hs, true, true, false, conf)
        res shouldEqual HttpResponse(302)
          .withHeaders(`Location`("12") :: hs)
      }
      "send back a gif if pixelExpected is true" in {
        val (res, Nil) =
          service.buildHttpResponse(event, "k", Map.empty, hs, false, true, false, conf)
        res shouldEqual HttpResponse(200)
          .withHeaders(hs)
          .withEntity(HttpEntity(contentType = ContentType(MediaTypes.`image/gif`),
            bytes = CollectorService.pixel))
      }
      "send back a found if pixelExpected and bounce is true" in {
        val (res, Nil) =
          service.buildHttpResponse(event, "k", Map.empty, hs, false, true, true, conf)
        res shouldEqual HttpResponse(302)
          .withHeaders(hs)
      }
      "send back ok otherwise" in {
        val (res, Nil) =
          service.buildHttpResponse(event, "k", Map.empty, hs, false, false, false, conf)
        res shouldEqual HttpResponse(200, entity = "ok")
          .withHeaders(hs)
      }
    }

    "buildUsualHttpResponse" in {
      "send back a found if pixelExpected and bounce is true" in {
        service.buildUsualHttpResponse(true, true) shouldEqual HttpResponse(302)
      }
      "send back a gif if pixelExpected is true" in {
        service.buildUsualHttpResponse(true, false) shouldEqual HttpResponse(200)
          .withEntity(HttpEntity(contentType = ContentType(MediaTypes.`image/gif`),
            bytes = CollectorService.pixel))
      }
      "send back ok otherwise" in {
        service.buildUsualHttpResponse(false, true) shouldEqual HttpResponse(200, entity = "ok")
      }
    }

    "buildRedirectHttpResponse" in {
      "give back a 302 if redirecting and there is a u query param" in {
        val (res, Nil) = service.buildRedirectHttpResponse(event, "k", Map("u" -> "12"))
        res shouldEqual HttpResponse(302).withHeaders(`Location`("12"))
      }
      /* scalaz incompat
      "give back a 400 if redirecting and there are no u query params" in {
        val (res, _) = service.buildRedirectHttpResponse(event, "k", Map.empty)
        res shouldEqual HttpResponse(400)
      }*/
      "the redirect url should not support a cookie replacement macro on redirect if not enabled" in {
        event.networkUserId = "1234"
        val (res, Nil) = service.buildRedirectHttpResponse(event, "k", Map("u" -> "http://localhost/?uid=${SP_NUID}"))
        res shouldEqual HttpResponse(302).withHeaders(`Location`("http://localhost/?uid=${SP_NUID}"))
      }
      "the redirect url should support a cookie replacement macro on redirect if enabled" in {
        val redirectService = new CollectorService(
          TestUtils.testConf.copy(redirectMacro = TestUtils.testConf.redirectMacro.copy(enabled = true)),
          CollectorSinks(new TestSink, new TestSink)
        )
        event.networkUserId = "1234"
        val (res, Nil) = redirectService.buildRedirectHttpResponse(event, "k", Map("u" -> "http://localhost/?uid=${SP_NUID}"))
        res shouldEqual HttpResponse(302).withHeaders(`Location`("http://localhost/?uid=1234"))
      }
      "the redirect url should allow for custom token placeholders" in {
        val redirectService = new CollectorService(
          TestUtils.testConf.copy(redirectMacro = TestUtils.testConf.redirectMacro.copy(enabled = true, Option.apply("[TOKEN]"))),
          CollectorSinks(new TestSink, new TestSink)
        )
        event.networkUserId = "1234"
        val (res, Nil) = redirectService.buildRedirectHttpResponse(event, "k", Map("u" -> "http://localhost/?uid=[TOKEN]"))
        res shouldEqual HttpResponse(302).withHeaders(`Location`("http://localhost/?uid=1234"))
      }
    }

    "cookieHeader" in {
      "give back a cookie header with the appropriate configuration" in {
        val nuid = "nuid"
        val conf = CookieConfig(true, "name", 5.seconds, Some("domain"))
        val Some(`Set-Cookie`(cookie)) = service.cookieHeader(Some(conf), nuid)
        cookie.name shouldEqual conf.name
        cookie.value shouldEqual nuid
        cookie.domain shouldEqual conf.domain
        cookie.path shouldEqual Some("/")
        cookie.expires must beSome
        (cookie.expires.get - DateTime.now.clicks).clicks must beCloseTo(conf.expiration.toMillis, 1000L)
      }
      "give back None if no configuration is given" in {
        service.cookieHeader(None, "nuid") shouldEqual None
      }
    }

    "bounceLocationHeader" in {
      "build a location header if bounce is true" in {
        val header = service.bounceLocationHeader(Map("a" -> "b"), Uri("st"), "bounce", true)
        header shouldEqual Some(`Location`("st?a=b&bounce=true"))
      }
      "give back none otherwise" in {
        val header = service.bounceLocationHeader(Map("a" -> "b"), Uri("st"), "bounce", false)
        header shouldEqual None
      }
    }

    "headers" in {
      "filter out the non Remote-Address and Raw-Request-URI headers" in {
        val request = HttpRequest()
          .withHeaders(List(
            `Location`("a"),
            `Remote-Address`(RemoteAddress.Unknown),
            `Raw-Request-URI`("uri")
          ))
        service.headers(request) shouldEqual List(`Location`("a").toString)
      }
    }

    "ipAndPartitionkey" in {
      "give back the ip and partition key as ip if remote address is defined" in {
        val address = RemoteAddress(InetAddress.getByName("localhost"))
        service.ipAndPartitionKey(address, true) shouldEqual(("127.0.0.1", "127.0.0.1"))
      }
      "give back the ip and a uuid as partition key if ipAsPartitionKey is false" in {
        val address = RemoteAddress(InetAddress.getByName("localhost"))
        val (ip, pkey) = service.ipAndPartitionKey(address, false)
        ip shouldEqual "127.0.0.1"
        pkey must beMatching(uuidRegex)
      }
      "give back unknown as ip and a random uuid as partition key if the address isn't known" in {
        val (ip, pkey) = service.ipAndPartitionKey(RemoteAddress.Unknown, true)
        ip shouldEqual "unknown"
        pkey must beMatching(uuidRegex)
      }
    }

    "netwokUserId" in {
      "give back the nuid query param if present" in {
        service.networkUserId(
          HttpRequest().withUri(Uri().withRawQueryString("nuid=12")),
          Some(HttpCookie("nuid", "13"))
        ) shouldEqual Some("12")
      }
      "give back the request cookie if there no nuid query param" in {
        service.networkUserId(HttpRequest(), Some(HttpCookie("nuid", "13"))) shouldEqual Some("13")
      }
      "give back none otherwise" in {
        service.networkUserId(HttpRequest(), None) shouldEqual None
      }
    }

    "accessControlAllowOriginHeader" in {
      "give a restricted ACAO header if there is an Origin header in the request" in {
        val origin = HttpOrigin("http", Host("origin"))
        val request = HttpRequest().withHeaders(`Origin`(origin))
        service.accessControlAllowOriginHeader(request) shouldEqual
          `Access-Control-Allow-Origin`(HttpOriginRange.Default(List(origin)))
      }
      "give an open ACAO header if there are no Origin headers in the request" in {
        val request = HttpRequest()
        service.accessControlAllowOriginHeader(request) shouldEqual
          `Access-Control-Allow-Origin`(HttpOriginRange.`*`)
      }
    }
  }
}