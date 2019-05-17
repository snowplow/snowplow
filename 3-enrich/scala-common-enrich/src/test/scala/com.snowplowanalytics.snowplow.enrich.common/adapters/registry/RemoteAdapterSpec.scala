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

import com.snowplowanalytics.snowplow.enrich.common.loaders.{
  CollectorApi,
  CollectorContext,
  CollectorPayload,
  CollectorSource
}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization.write
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.specification.BeforeAfter
import scalaz.Scalaz._
import scalaz.{Failure, NonEmptyList, Success}

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class RemoteAdapterSpec extends Specification with ValidationMatchers {

  override def is =
    sequential ^
      s2"""
   This is a specification to test the RemoteAdapter functionality.
   RemoteAdapter must return any events parsed by this local test adapter                        ${testWrapperLocal(e1)}
   This local enricher (well, any remote enricher) must treat an empty list as an error          ${testWrapperLocal(e2)}
   RemoteAdapter must also return any other errors issued by this local adapter                  ${testWrapperLocal(e3)}
   HTTP response contains string that is not a correct JSON, should fail                         ${testWrapperLocal(e4)}
   HTTP response contains empty string, should fail                                              ${testWrapperLocal(e5)}
   HTTP response contains well-formatted JSON but without events and error, will fail            ${testWrapperLocal(e6)}
   HTTP response contains well-formatted JSON, events not List[Map[String, String]], fail        ${testWrapperLocal(e7)}
   HTTP response contains well-formatted JSON, events that contains an empty list, will fail     ${testWrapperLocal(e8)}
   HTTP response contains status code other than 200, fail                                       ${testWrapperLocal(e9)}
   """

  implicit val resolver = SpecHelpers.IgluResolver

  val actionTimeout = Duration(5, TimeUnit.SECONDS)

  val mockTracker       = "testTracker-v0.1"
  val mockPlatform      = "srv"
  val mockSchemaKey     = "moodReport"
  val mockSchemaVendor  = "org.remoteEnricherTest"
  val mockSchemaName    = "moodChange"
  val mockSchemaFormat  = "jsonschema"
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
    val sampleTracker      = "testTracker-v0.1"
    val samplePlatform     = "srv"
    val sampleSchemaKey    = "moodReport"
    val sampleSchemaVendor = "org.remoteEnricherTest"
    val sampleSchemaName   = "moodChange"

    implicit val formats = DefaultFormats

    sealed case class Payload(
      queryString: Map[String, String],
      headers: List[String],
      body: Option[String],
      contentType: Option[String]
    )

    sealed case class Response(
      events: List[Map[String, String]],
      error: String
    )

    def handle(body: String) =
      try {
        parse(body).extract[Payload] match {
          case payload: Payload =>
            parse(payload.body.get) \ "mood" match {
              case JArray(list) =>
                val output = list.map { item =>
                  val json =
                    ("schema" -> "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0") ~
                      ("data"   -> (("schema" -> s"iglu:$mockSchemaVendor/$mockSchemaName/$mockSchemaFormat/$mockSchemaVersion") ~
                        ("data" -> item)))

                  Map(("tv"    -> sampleTracker),
                      ("e"     -> "ue"),
                      ("p"     -> payload.queryString.getOrElse("p", samplePlatform)),
                      ("ue_pr" -> write(json))) ++ payload.queryString
                }
                write(Response(output, null))
              case _ => write("server error") // an example case for non 200 response
            }
          case anythingElse =>
            write(Response(null, s"expecting a payload json but got a ${anythingElse.getClass}"))
        }
      } catch {
        case NonFatal(e) => write(Response(null, s"aack, sampleAdapter exception $e"))
      }

  }

  object Shared {
    val api       = CollectorApi("org.remoteEnricherTest", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
                                   "37.157.33.123".some,
                                   None,
                                   None,
                                   List("testHeader: testValue"),
                                   None)
  }

  var testAdapter: RemoteAdapter = _

  object testWrapperLocal extends BeforeAfter {
    val mockServerPort         = 8091
    val mockServerPath         = "myEnrichment"
    var httpServer: HttpServer = _

    def before = {
      httpServer = localHttpServer(mockServerPort, mockServerPath)
      httpServer.start()

      testAdapter = new RemoteAdapter(s"http://localhost:$mockServerPort/$mockServerPath", Some(1000L), Some(5000L))
    }

    def after =
      httpServer.stop(0)
  }

  def e1 = {
    val eventData    = List(("anonymous", -0.3), ("subscribers", 0.6))
    val eventsAsJson = eventData.map(evt => s"""{"${evt._1}":${evt._2}}""")

    val payloadBody = s""" {"mood": [${eventsAsJson.mkString(",")}]} """
    val payload     = CollectorPayload(Shared.api, Nil, None, payloadBody.some, Shared.cljSource, Shared.context)

    val expected = eventsAsJson
      .map(
        evtJson =>
          RawEvent(
            Shared.api,
            Map(
              "tv"    -> mockTracker,
              "e"     -> "ue",
              "p"     -> mockPlatform,
              "ue_pr" -> s"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:$mockSchemaVendor/$mockSchemaName/$mockSchemaFormat/$mockSchemaVersion","data":$evtJson}}"""
            ),
            None,
            Shared.cljSource,
            Shared.context
        ))
      .toNel
      .get

    val they = testAdapter.toRawEvents(payload)
    they must beSuccessful(expected)
  }

  def e2 = {
    val emptyListPayload =
      CollectorPayload(Shared.api, Nil, None, "".some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Missing payload body")
    testAdapter.toRawEvents(emptyListPayload) must beFailing(expected)
  }

  def e3 = {
    val bodylessPayload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected        = NonEmptyList("Missing payload body")
    testAdapter.toRawEvents(bodylessPayload) must beFailing(expected)
  }

  def e4 = {
    val bodylessPayload     = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val invalidJsonResponse = Success("{invalid json")
    val result              = testAdapter.processResponse(bodylessPayload, invalidJsonResponse)
    val expected = NonEmptyList(
      "Json is not parsable, error: com.fasterxml.jackson.core.JsonParseException: Unexpected character ('i' (code 105)): was expecting double-quote to start field name\n at [Source: (String)\"{invalid json\"; line: 1, column: 3] - response: {invalid json")
    result must beFailing(expected)
  }

  def e5 = {
    val bodylessPayload   = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val emptyJsonResponse = Success("")
    val expected          = NonEmptyList("Empty response")
    val result            = testAdapter.processResponse(bodylessPayload, emptyJsonResponse)
    result must beFailing(expected)
  }

  def e6 = {
    val bodylessPayload        = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val unexpectedJsonResponse = Success("{\"invalid\":\"response\"}")
    val result                 = testAdapter.processResponse(bodylessPayload, unexpectedJsonResponse)
    val expected               = NonEmptyList("Incompatible response, missing error and events fields")
    result must beFailing(expected)
  }

  def e7 = {
    val bodylessPayload           = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val badStrucutredJsonResponse = Success("{\"events\":\"response\"}")
    val result                    = testAdapter.processResponse(bodylessPayload, badStrucutredJsonResponse)
    val expected = NonEmptyList(
      "The events field should be List[Map[String, String]], error: org.json4s.package$MappingException: Expected collection but got JString(response) for root JString(response) and mapping List[Map[String, String]] - response: {\"events\":\"response\"}")
    result must beFailing(expected)
  }

  def e8 = {
    val bodylessPayload   = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val emptyJsonResponse = Success("{\"error\":\"\", \"events\":[]}")
    val result            = testAdapter.processResponse(bodylessPayload, emptyJsonResponse)
    val expected          = NonEmptyList("Unable to parse response: {\"error\":\"\", \"events\":[]}")
    result must beFailing(expected)
  }

  def e9 = {
    val payloadBody     = s""" {"non-mood": []} """
    val bodylessPayload = CollectorPayload(Shared.api, Nil, None, payloadBody.some, Shared.cljSource, Shared.context)
    val result          = testAdapter.toRawEvents(bodylessPayload)
    val expected        = NonEmptyList("Request failed with status 500 and body \"server error\"")
    result must beFailing(expected)
  }
}
