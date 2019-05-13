/*
 * Copyright (c) 2019-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers
import org.specs2.specification.BeforeAfter

import loaders.{CollectorPayload => CP, _}
import utils.Clock._

class RemoteAdapterSpec extends Specification with ValidatedMatchers {

  override def is =
    sequential ^
      s2"""
   This is a specification to test the RemoteAdapter functionality.
   RemoteAdapter must return any events parsed by this local test adapter                        ${testWrapperLocal(
        e1
      )}
   This local enricher (well, any remote enricher) must treat an empty list as an error          ${testWrapperLocal(
        e2
      )}
   RemoteAdapter must also return any other errors issued by this local adapter                  ${testWrapperLocal(
        e3
      )}
   HTTP response contains string that is not a correct JSON, should fail                         ${testWrapperLocal(
        e4
      )}
   HTTP response contains well-formatted JSON but without events and error, will fail            ${testWrapperLocal(
        e5
      )}
   HTTP response contains well-formatted JSON, events that contains an empty list, will fail     ${testWrapperLocal(
        e6
      )}
   """

  val actionTimeout = Duration(5, TimeUnit.SECONDS)

  val mockTracker = "testTracker-v0.1"
  val mockPlatform = "srv"
  val mockSchemaKey = "moodReport"
  val mockSchemaVendor = "org.remoteEnricherTest"
  val mockSchemaName = "moodChange"
  val mockSchemaFormat = "jsonschema"
  val mockSchemaVersion = "1-0-0"

  private def localHttpServer(tcpPort: Int, basePath: String): HttpServer = {
    val httpServer = HttpServer.create(new InetSocketAddress(tcpPort), 0)

    httpServer.createContext(
      s"/$basePath",
      new HttpHandler {
        def handle(exchange: HttpExchange): Unit = {

          val response = MockRemoteAdapter.handle(getBodyAsString(exchange.getRequestBody))
          if (response != "\"server error\"") {
            exchange.sendResponseHeaders(200, 0)
          } else {
            exchange.sendResponseHeaders(500, 0)
          }
          exchange.getResponseBody.write(response.getBytes)
          exchange.getResponseBody.close()
        }
      }
    )
    httpServer
  }

  private def getBodyAsString(body: InputStream): String = {
    val s = new java.util.Scanner(body).useDelimiter("\\A")
    if (s.hasNext) s.next() else ""
  }

  object MockRemoteAdapter {
    val sampleTracker = "testTracker-v0.1"
    val samplePlatform = "srv"
    val sampleSchemaKey = "moodReport"
    val sampleSchemaVendor = "org.remoteEnricherTest"
    val sampleSchemaName = "moodChange"

    def handle(body: String): String =
      (for {
        js <- parse(body).leftMap(_.message)
        payload <- js.as[Payload].leftMap(_.message)
        payloadBody <- payload.body.toRight("no payload body")
        payloadBodyJs <- parse(payloadBody).leftMap(_.message)
        array <- payloadBodyJs.asArray.toRight("payload body is not an array")
        events = array.map { item =>
          val json = Json.obj(
            "schema" := "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data" := Json.obj(
              "schema" := s"iglu:$mockSchemaVendor/$mockSchemaName/$mockSchemaFormat/$mockSchemaVersion",
              "data" := item
            )
          )
          Map(
            "tv" -> sampleTracker,
            "e" -> "ue",
            "p" -> payload.queryString.getOrElse("p", samplePlatform),
            "ue_pr" -> json.noSpaces
          ) ++ payload.queryString
        }
      } yield events) match {
        case Right(es) => Response(es.toList.some, None).asJson.noSpaces
        case Left(f) => Response(None, s"server error: $f".some).asJson.noSpaces
      }
  }

  object Shared {
    val api = CollectorApi("org.remoteEnricherTest", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      List("testHeader: testValue"),
      None
    )
  }

  var testAdapter: RemoteAdapter = _

  object testWrapperLocal extends BeforeAfter {
    val mockServerPort = 8091
    val mockServerPath = "myEnrichment"
    var httpServer: HttpServer = _

    def before = {
      httpServer = localHttpServer(mockServerPort, mockServerPath)
      httpServer.start()

      testAdapter = new RemoteAdapter(
        s"http://localhost:$mockServerPort/$mockServerPath",
        Some(1000L),
        Some(5000L)
      )
    }

    def after =
      httpServer.stop(0)
  }

  def e1 = {
    val eventData = NonEmptyList.of(("anonymous", -0.3), ("subscribers", 0.6))
    val eventsAsJson = eventData.map(evt => s"""{"${evt._1}":${evt._2}}""")

    val payloadBody = s"""[${eventsAsJson.toList.mkString(",")}]"""
    val payload =
      CP(Shared.api, Nil, None, payloadBody.some, Shared.cljSource, Shared.context)

    val expected = eventsAsJson
      .map { evtJson =>
        RawEvent(
          Shared.api,
          Map(
            "tv" -> mockTracker,
            "e" -> "ue",
            "p" -> mockPlatform,
            "ue_pr" -> s"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:$mockSchemaVendor/$mockSchemaName/$mockSchemaFormat/$mockSchemaVersion","data":$evtJson}}"""
          ),
          None,
          Shared.cljSource,
          Shared.context
        )
      }

    val they = testAdapter.toRawEvents(payload, SpecHelpers.client).value
    they must beValid(expected)
  }

  def e2 = {
    val emptyListPayload =
      CP(Shared.api, Nil, None, "".some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList.one(
      FailureDetails.AdapterFailure.InputData(
        "body",
        None,
        "empty body: not a valid remote adapter http://localhost:8091/myEnrichment payload"
      )
    )
    testAdapter.toRawEvents(emptyListPayload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e3 = {
    val bodylessPayload =
      CP(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected = NonEmptyList.one(
      FailureDetails.AdapterFailure.InputData(
        "body",
        None,
        "empty body: not a valid remote adapter http://localhost:8091/myEnrichment payload"
      )
    )
    testAdapter.toRawEvents(bodylessPayload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e4 = {
    val bodylessPayload =
      CP(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val invalidJsonResponse = Right("{invalid json")
    val result = testAdapter.processResponse(bodylessPayload, invalidJsonResponse)
    val expected = FailureDetails.AdapterFailure.NotJson(
      "body",
      Some("{invalid json"),
      """invalid json: expected " got 'invali...' (line 1, column 2)"""
    )
    result must beLeft(expected)
  }

  def e5 = {
    val bodylessPayload =
      CP(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val unexpectedJsonResponse = Right("{\"events\":\"response\"}")
    val result = testAdapter.processResponse(bodylessPayload, unexpectedJsonResponse)
    val expected = FailureDetails.AdapterFailure.InputData(
      "body",
      Some("""{"events":"response"}"""),
      "could not be decoded as a list of json objects: C[A]: DownField(events)"
    )
    result must beLeft(expected)
  }

  def e6 = {
    val bodylessPayload =
      CP(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val emptyJsonResponse = Right("{\"error\":\"\", \"events\":[]}")
    val result = testAdapter.processResponse(bodylessPayload, emptyJsonResponse)
    val expected = FailureDetails.AdapterFailure.InputData(
      "body",
      Some("""{"error":"", "events":[]}"""),
      "empty list of events"
    )
    result must beLeft(expected)
  }
}

final case class Payload(
  queryString: Map[String, String],
  headers: List[String],
  body: Option[String],
  contentType: Option[String]
)

final case class Response(events: Option[List[Map[String, String]]], error: Option[String])
