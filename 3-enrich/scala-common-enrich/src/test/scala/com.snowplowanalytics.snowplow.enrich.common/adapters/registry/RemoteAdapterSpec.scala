package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{
  CollectorApi,
  CollectorContext,
  CollectorPayload,
  CollectorSource
}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.joda.time.DateTime
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.specification.{Before, BeforeAfter}
import scalaz.Scalaz._
import scalaz.{Failure, NonEmptyList, Success}

import scala.concurrent.duration.Duration

class RemoteAdapterSpec extends Specification with ValidationMatchers {

  // Connections to external webservices are outside the scope of normal unit tests, so they are normally disabled here.
  // But if you happen to have a test remote enrichment webservice running somewhere that behaves like the localHttpServer below,
  // you can specify its url here:
  private val externalEnrichmentUrl  = None //e.g. Some("http://127.0.0.1:8995/myEnrichment")
  private val externalActionTimeout  = Duration(5, TimeUnit.SECONDS)
  private def shouldRunExternalTests = externalEnrichmentUrl.isDefined

  override def is = sequential ^ s2"""
   This is a specification to test the RemoteAdapter functionality.
   RemoteAdapter must return any events parsed by this local test enricher                 ${testWrapperLocal(e1)}
   this local enricher (well, any remote enricher) must treat an empty list as an error    ${testWrapperLocal(e2)}
   RemoteAdapter must also return any other errors issued by this local enricher           ${testWrapperLocal(e3)}
   RemoteAdapter must also return multiple errors issued by this local enricher            ${testWrapperLocal(e4)}
   RemoteAdapter must return any events parsed by an external test enricher                ${testWrapperExternal(e1ext)}
   that external test enricher must treat an empty list as an error                        ${testWrapperExternal(e2ext)}
   RemoteAdapter must also return any other errors issued by that external enricher        ${testWrapperExternal(e3ext)}
   RemoteAdapter must also return multiple errors issued by that external enricher         ${testWrapperExternal(e4ext)}
   """

  implicit val resolver = SpecHelpers.IgluResolver

  val actionTimeout = Duration(5, TimeUnit.SECONDS)

  val mockTracker          = "testTracker-v0.1"
  val mockPlatform         = "srv"
  val mockSchemaKey        = "moodReport"
  val mockSchemaVendor     = "org.remoteEnricherTest"
  val mockSchemaName       = "moodChange"
  val mockSchemaFormat     = "jsonschema"
  val mockSchemaVersion    = "1-0-0"
  val bodyMissingErrorText = "missing payload body"
  val emptyListErrorText   = "no events were found in payload body"
  val doubleErrorText      = List("error one", "error two")

  private def localHttpServer(tcpPort: Int, basePath: String): HttpServer = {
    val httpServer = HttpServer.create(new InetSocketAddress(tcpPort), 0)

    httpServer.createContext(
      s"/$basePath",
      new HttpHandler {
        def handle(exchange: HttpExchange): Unit = {

          val encodedObj = getBodyAsString(exchange.getRequestBody)
          val obj        = RemoteAdapter.deserializeFromBase64(encodedObj)
          val response = obj match {
            case payload: CollectorPayload =>
              val parsedEvents = MockRemoteAdapter.toRawEvents(payload)

              parsedEvents match {
                case Success(events) => Right(events.head :: events.tail)
                case Failure(msgs)   => Left(msgs.head :: msgs.tail)
              }
            case _ => Left("ugh".failNel)
          }

          exchange.sendResponseHeaders(200, 0)
          exchange.getResponseBody.write(RemoteAdapter.serializeToBase64(response).getBytes)
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

  object MockRemoteAdapter extends Adapter {

    private val EventSchemaMap = Map(
      mockSchemaKey -> SchemaKey(mockSchemaVendor, mockSchemaName, mockSchemaFormat, mockSchemaVersion).toSchemaUri
    )

    override def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
      if (payload.body.isEmpty) {
        bodyMissingErrorText.failNel

      } else if (payload.body.get == "") {
        doubleErrorText.toNel.get.fail

      } else {
        parse(payload.body.get) \ "mood" match {
          case JArray(list) =>
            val schema = lookupSchema(mockSchemaKey.some, "", 0, EventSchemaMap)

            val events = list.map { event =>
              RawEvent(
                api = payload.api,
                parameters = toUnstructEventParams(mockTracker,
                                                   toMap(payload.querystring),
                                                   schema.toOption.get,
                                                   event,
                                                   mockPlatform),
                contentType = payload.contentType,
                source      = payload.source,
                context     = payload.context
              ).success
            }
            if (events.isEmpty)
              emptyListErrorText.failNel
            else
              rawEventsListProcessor(events)

          case _ => "ng".failNel
        }
      }
  }

  object Shared {
    val api       = CollectorApi("org.remoteEnricherTest", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
                                   "37.157.33.123".some,
                                   None,
                                   None,
                                   Nil,
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

      testAdapter = new RemoteAdapter(s"http://localhost:$mockServerPort/$mockServerPath", actionTimeout)
    }

    def after =
      httpServer.stop(0)
  }

  object testWrapperExternal extends Before {

    def before =
      if (shouldRunExternalTests) {
        testAdapter = new RemoteAdapter(externalEnrichmentUrl.get, externalActionTimeout)
      }

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

    testAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val emptyListPayload =
      CollectorPayload(Shared.api, Nil, None, "{\"mood\":[]}".some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(emptyListErrorText)
    testAdapter.toRawEvents(emptyListPayload) must beFailing(expected)
  }

  def e3 = {
    val bodylessPayload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected        = NonEmptyList(bodyMissingErrorText)
    testAdapter.toRawEvents(bodylessPayload) must beFailing(expected)
  }

  def e4 = {
    val blankPayload = CollectorPayload(Shared.api, Nil, None, "".some, Shared.cljSource, Shared.context)
    val expected     = doubleErrorText.toNel.get
    testAdapter.toRawEvents(blankPayload) must beFailing(expected)
  }

  def e1ext =
    if (shouldRunExternalTests)
      e1
    else
      ok

  def e2ext =
    if (shouldRunExternalTests)
      e2
    else
      ok

  def e3ext =
    if (shouldRunExternalTests)
      e3
    else
      ok

  def e4ext =
    if (shouldRunExternalTests)
      e4
    else
      ok

}
