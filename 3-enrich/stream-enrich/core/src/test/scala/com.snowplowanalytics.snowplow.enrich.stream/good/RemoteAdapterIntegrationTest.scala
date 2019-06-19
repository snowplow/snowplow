/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.stream
package good

import java.io.InputStream
import java.net.InetSocketAddress

import cats.syntax.either._
import cats.syntax.option._
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.{
  CollectorPayload => CollectorPayload1
}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.thrift.TSerializer
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import SpecHelpers._

final case class Payload(
  queryString: Map[String, String],
  headers: List[String],
  body: Option[String],
  contentType: Option[String]
)

final case class Response(events: Option[List[Map[String, String]]], error: Option[String])

// This integration test instantiates an HTTP server acting like a remote adapter
// and creates payloads to be sent to it.
object RemoteAdapterIntegrationTest {

  val transactionId = "123456" // added to the event by the remote adapter

  def localHttpAdapter(tcpPort: Int, basePath: String = ""): HttpServer = {
    def _handle(body: String): String =
      (for {
        json <- parse(body).leftMap(_ => "not json")
        payload <- json.as[Payload].leftMap(_ => "doesn't match payload format")
      } yield List(payload.queryString ++ Map("tid" -> transactionId)))
        .fold(
          f => Response(None, f.some),
          l => Response(l.some, None)
        )
        .asJson
        .noSpaces

    def inputStreamToString(is: InputStream): String = {
      val s = new java.util.Scanner(is).useDelimiter("\\A")
      if (s.hasNext) s.next() else ""
    }

    val localAdapter = HttpServer.create(new InetSocketAddress(tcpPort), 0)
    localAdapter.createContext(
      s"/$basePath",
      new HttpHandler {
        def handle(exchange: HttpExchange): Unit = {
          val bodyStr = inputStreamToString(exchange.getRequestBody)
          val response = _handle(bodyStr)
          exchange.sendResponseHeaders(200, 0)
          exchange.getResponseBody.write(response.getBytes)
          exchange.getResponseBody.close()
        }
      }
    )
    localAdapter
  }
}

class RemoteAdapterIntegrationTest extends Specification with BeforeAfterAll {
  import RemoteAdapterIntegrationTest._

  val localAdapter: HttpServer = localHttpAdapter(9090)

  def beforeAll() = localAdapter.start()

  def afterAll() = localAdapter.stop(0)

  val ThriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }
  val serializer = ThriftSerializer.get()

  val e = new CollectorPayload1(
    "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
    "79.213.165.223",
    System.currentTimeMillis,
    "UTF-8",
    "cloudfront"
  )
  e.path = "/remoteVendor/v42" // path that will lead to remote adapter, same as in SpecHelpers
  e.querystring = "&e=pp&pp_mix=0&pp_max=7&pp_miy=0&pp_may=746" // page ping event

  sequential
  "Stream Enrich" should {

    "be able to send payloads to a remote HTTP adapter and the enriched events should contain fields added by the remote adapter" in {

      e.body = "{}" // required by remote adapter
      val goodPayload = serializer.serialize(e)

      val expected = List[StringOrRegex](
        "",
        "",
        TimestampRegex,
        TimestampRegex,
        "",
        "page_ping",
        Uuid4Regexp,
        "123456",
        "",
        "",
        "cloudfront",
        EnrichVersion,
        "",
        "fbc9cb674bbaeb0dfe13b743bc043790928931e1",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "0",
        "7",
        "0",
        "746",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        TimestampRegex,
        "com.snowplowanalytics.snowplow",
        "page_ping",
        "jsonschema",
        "1-0-0",
        "",
        ""
      )

      val enrichedEvent = TestSource.enrichEvents(goodPayload)(0)
      enrichedEvent.isValid must beTrue

      // "-1" prevents empty strings from being discarded from the end of the array
      val fields = enrichedEvent.toOption.get._1.split("\t", -1)
      fields.contains(transactionId) must beTrue // added by remote adapter
      fields.size must beEqualTo(StructEventSpec.expected.size)
      Result.unit(
        {
          for (idx <- StructEventSpec.expected.indices) {
            fields(idx) must beFieldEqualTo(expected(idx), withIndex = idx)
          }
        }
      )
    }

    "be able to send payloads to a remote HTTP adapter and handle a problem on the remote adapter" in {
      e.body = null // required by the remote adapter
      val badPayload = serializer.serialize(e)

      val enrichedEvent = TestSource.enrichEvents(badPayload)(0)
      enrichedEvent.isValid must beFalse
    }
  }
}
