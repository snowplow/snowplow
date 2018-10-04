package com.example

import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedRawEvents
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import scalaz.Scalaz._
import scalaz.{Failure, Success}

class MockAdapter(resolverConfFilename: String) extends Adapter {

  val mockTracker      = "testTracker-v0.1"
  val mockPlatform     = "srv"
  val mockSchemaKey    = "moodReport"
  val mockSchemaVendor = "org.remoteEnricherTest"
  val mockSchemaName   = "moodChange"

  val bodyMissingErrorText = "missing payload body"
  val emptyListErrorText   = "no events were found in payload body"
  val doubleErrorText      = List("error one", "error two")

  private val EventSchemaMap = Map(
    mockSchemaKey -> SchemaKey(mockSchemaVendor, mockSchemaName, "jsonschema", "1-0-0").toSchemaUri
  )

  implicit val resolver = IgluUtils.getResolver(resolverConfFilename)

  def handle(decodedBody: Any) =
    try {
      decodedBody match {
        case payload: CollectorPayload =>
          val parsedEvents = toRawEvents(payload)

          parsedEvents match {
            case Success(events) => Right(events.head :: events.tail)
            case Failure(msgs)   => Left(msgs.head :: msgs.tail)
          }

        case anythingElse =>
          Left(List(s"expecting a CollectorPayload, but got a ${anythingElse.getClass}"))
      }
    } catch {
      case e: Exception => Left(List(s"aack, MockAdapter exception $e"))
    }

  override def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    if (payload.body.isEmpty) {
      bodyMissingErrorText.failNel

    } else if (payload.body.get == "") {
      doubleErrorText.toNel.get.fail

    } else {
      parse(payload.body.get) \ "mood" match {
        case JArray(list) =>
          val schema = lookupSchema(mockSchemaKey.some, "Listener", 0, EventSchemaMap)

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
