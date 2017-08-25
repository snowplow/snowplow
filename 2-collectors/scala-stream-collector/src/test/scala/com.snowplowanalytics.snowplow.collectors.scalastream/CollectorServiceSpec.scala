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
package com.snowplowanalytics.snowplow
package collectors.scalastream

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import org.apache.thrift.TSerializer
import org.specs2.mutable.Specification

import CollectorPayload.thrift.model1.CollectorPayload
import generated.Settings
import model._

class CollectorServiceSpec extends Specification {
  val service = new CollectorService(
    TestUtils.testConf,
    CollectorSinks(new TestSink, new TestSink)
  )
  val uuidRegex = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".r
  val event = new CollectorPayload(
    "iglu-schema", "ip", System.currentTimeMillis, "UTF-8", "collector" )
  val hs = List(`Raw-Request-URI`("uri"))
  val serializer = new TSerializer()

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
    }

    "preflightResponse" in {
      "return a response appropriate to cors preflight options requests" in {
        val r = HttpRequest()
        service.preflightResponse(r) shouldEqual HttpResponse()
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
        e.collector shouldEqual s"${Settings.shortName}-${Settings.version}-stdout"
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
        e.collector shouldEqual s"${Settings.shortName}-${Settings.version}-stdout"
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
      "rely on buildRedirectHttpResponse if the path starts with /r/" in {
        val (res, Nil) = service.buildHttpResponse(event, "k", "u=12", "/r/a", hs, true)
        res shouldEqual HttpResponse(302)
          .withHeaders(`Location`("12") :: hs)
      }
      "send back a gif if pixelExpected is true" in {
        val (res, Nil) = service.buildHttpResponse(event, "k", "", "/a/b", hs, true)
        res shouldEqual HttpResponse(200)
          .withHeaders(hs)
          .withEntity(HttpEntity(contentType = ContentType(MediaTypes.`image/gif`),
            bytes = CollectorService.pixel))
      }
      "send back ok otherwise" in {
        val (res, Nil) = service.buildHttpResponse(event, "k", "", "/a/b", hs, false)
        res shouldEqual HttpResponse(200, entity = "ok")
          .withHeaders(hs)
      }
    }

    "buildRedirectHttpResponse" in {
      "give back a 302 if redirecting and there is a u query param" in {
        val (res, Nil) = service.buildRedirectHttpResponse(event, "k", "u=12")
        res shouldEqual HttpResponse(302).withHeaders(`Location`("12"))
      }
      /* scalaz incompat
      "give back a 400 if redirecting and there are no u query params" in {
        val (res, _) = service.buildRedirectHttpResponse(event, "k", "")
        res shouldEqual HttpResponse(400)
      }*/
    }

    "getCookieHeader" in {
      "give back a cookie header with the appropriate configuration" in {
        val nuid = "nuid"
        val conf = CookieConfig(true, "name", 5.seconds, Some("domain"))
        val Some(`Set-Cookie`(cookie)) = service.getCookieHeader(Some(conf), nuid)
        cookie.name shouldEqual conf.name
        cookie.value shouldEqual nuid
        cookie.domain shouldEqual conf.domain
        cookie.path shouldEqual Some("/")
        cookie.expires must beSome
        (cookie.expires.get - DateTime.now.clicks).clicks must beCloseTo(conf.expiration.toMillis, 1000L)
      }
      "give back None if no configuration is given" in {
        service.getCookieHeader(None, "nuid") shouldEqual None
      }
    }

    "getHeaders" in {
      "filter out the non Remote-Address and Raw-Request-URI headers" in {
        val request = HttpRequest()
          .withHeaders(List(
            `Location`("a"),
            `Remote-Address`(RemoteAddress.Unknown),
            `Raw-Request-URI`("uri")
          ))
        service.getHeaders(request) shouldEqual List(`Location`("a").toString)
      }
    }

    "getIpAndPartitionkey" in {
      "give back the ip and partition key as ip if remote address is defined" in {
        val address = RemoteAddress(InetAddress.getByName("localhost"))
        service.getIpAndPartitionKey(address, true) shouldEqual(("127.0.0.1", "127.0.0.1"))
      }
      "give back the ip and a uuid as partition key if ipAsPartitionKey is false" in {
        val address = RemoteAddress(InetAddress.getByName("localhost"))
        val (ip, pkey) = service.getIpAndPartitionKey(address, false)
        ip shouldEqual "127.0.0.1"
        pkey must beMatching(uuidRegex)
      }
      "give back unknown as ip and a random uuid as partition key if the address isn't known" in {
        val (ip, pkey) = service.getIpAndPartitionKey(RemoteAddress.Unknown, true)
        ip shouldEqual "unknown"
        pkey must beMatching(uuidRegex)
      }
    }

    "getNetwokUserId" in {
      "give back the nuid query param if present" in {
        service.getNetworkUserId(
          HttpRequest().withUri(Uri().withRawQueryString("nuid=12")),
          Some(HttpCookie("nuid", "13"))
        ) shouldEqual "12"
      }
      "give back the request cookie if there no nuid query params" in {
        service.getNetworkUserId(HttpRequest(), Some(HttpCookie("nuid", "13"))) shouldEqual "13"
      }
      "give back a random uuid if there are no cookies nor query params" in {
        service.getNetworkUserId(HttpRequest(), None) must beMatching(uuidRegex)
      }
    }

    "getAccessControlAllowOriginHeader" in {
      "give a restricted ACAO header if there is an Origin header in the request" in {
        val origin = HttpOrigin("http", Host("origin"))
        val request = HttpRequest().withHeaders(`Origin`(origin))
        service.getAccessControlAllowOriginHeader(request) shouldEqual
          `Access-Control-Allow-Origin`(HttpOriginRange.Default(List(origin)))
      }
      "give an open ACAO header if there are no Origin headers in the request" in {
        val request = HttpRequest()
        service.getAccessControlAllowOriginHeader(request) shouldEqual
          `Access-Control-Allow-Origin`(HttpOriginRange.`*`)
      }
    }
  }
}