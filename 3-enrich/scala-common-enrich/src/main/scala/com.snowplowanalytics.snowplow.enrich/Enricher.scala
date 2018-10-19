package com.snowplowanalytics.snowplow.enrich

import io.circe.Json
import org.joda.time.DateTime
import cats.data.{Validated, ValidatedNel}
import cats.data.Nested
import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.alternative._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.instances.either._
import cats.instances.list._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.repositories.{HttpRepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.snowplow.enrich.badrows.BadRow.TrackerProtocolViolation
import com.snowplowanalytics.snowplow.enrich.badrows.BadRowPayload.{CollectorMeta, RawEvent}
import com.snowplowanalytics.snowplow.enrich.common.{EtlPipeline, ValidatedEnrichedEvent}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders._

object Enricher {
  def catsNEL[A](list: scalaz.NonEmptyList[A]) =
    cats.data.NonEmptyList.fromListUnsafe(list.list)

  val resolver = Resolver(
    10,
    HttpRepositoryRef(RepositoryRefConfig("Iglu Central", 1, List("com.snowplow")), "http://iglucentral.com", None))
  val enrichmentRegistry = EnrichmentRegistry(Map.empty)
  val now                = DateTime.now()
  val loader             = CljTomcatLoader

  def parse(line: String): List[ValidatedEnrichedEvent] = {
    val payload = loader.toCollectorPayload(line) // This is our parseRawCollectorPayload
    val events  = EtlPipeline.processEvents(enrichmentRegistry, "badrows-test", now, payload)(resolver)

    val z = for {
      // First two steps are equal to EnrichPseudoCode.parseRawPayload
      collectorPayload <- loader.toCollectorPayload(line).fold(e => catsNEL(e).asLeft, _.asRight)
      _                <- middleware(collectorPayload)
      // Extracting valid raw events
      (trackerViolations, rawEvents) = splitCollectorPayload(collectorPayload).separate
      //
      (snowplowViolations, snowplowPayloads) = rawEvents.map(parseSnowplowPayload).separate
    } yield (trackerViolations, ???)
    events
  }

  case class RawEventPayload(event: Map[String, String], meta: CollectorMeta)

  /** Confirm that CollectorPayload contains valid POST */
  def middleware(payload: Option[CollectorPayload]) =
    payload match {
      case Some(CollectorPayload.TrackerPayload(api, qs, contentType, Some(body), source, context)) =>
        body.toData match {
          case Some(SelfDescribingData(key, _)) if key.vendor == "post_payload" =>
            Right(payload)
          case Some(SelfDescribingData(key, _)) =>
            Left(NonEmptyList.of(s"Unknown schema ${key.toSchemaUri}"))
          case None =>
            Left(NonEmptyList.of(s"Not a valid self-describing payload"))
        }
      // GET
      case Some(CollectorPayload.TrackerPayload(api, qs, contentType, None, source, context)) =>
        Right(payload)
      // Webhook
      case Some(CollectorPayload.WebhookPayload(api, qs, contentType, body, source, context)) =>
        Right(payload)
    }

  /**
   * Split single payload into individual events with collector metadata attached
   * Individual events still can have a lot of violations discovered downstream
   */
  def splitCollectorPayload(payload: Option[CollectorPayload]): List[Error[RawEvent]] =
    payload match {
      // POST
      case Some(p @ CollectorPayload.TrackerPayload(api, qs, contentType, Some(body), source, context)) =>
        val data   = body.toData.getOrElse(throw new RuntimeException("middleware")).data
        val events = extractEvents(data, CollectorMeta.fromPayload(p))
        Nested(events).map(raw => RawEvent(CollectorMeta(api, source, context), raw)).value
      // GET
      case Some(CollectorPayload.TrackerPayload(api, qs, contentType, None, source, context)) =>
        ???
      // Webhook
      case Some(CollectorPayload.WebhookPayload(api, qs, contentType, body, source, context)) =>
        ???

    }

  type Error[A]    = Either[TrackerProtocolViolation, A]
  type ErrorStr[A] = ValidatedNel[String, A]

  def parseSnowplowPayload(event: RawEvent): Either[String, RawEvent] = ???

  def extractEvents(json: Json, meta: CollectorMeta): List[Error[Map[String, String]]] =
    json.asArray match {
      case Some(jsonArray) =>
        jsonArray.map(extractEvent(_, meta)).toList
      case None =>
        throw new RuntimeException("middleware")
    }

  def extractEvent[A](json: Json, meta: CollectorMeta): Error[Map[String, String]] =
    json.asObject match {
      case Some(jsonObject) =>
        val fields = jsonObject.toList.traverse[ErrorStr, (String, String)] {
          case (k, v) =>
            v.asString match { // TODO: we're checking only the fact that each value is a string,
              // while can do more assumptions about TP: timestamps, base64, SD JSONs
              case Some(string) => Validated.Valid((k, string))
              case None         => Validated.invalidNel(s"$k should contain string, not ${v.noSpaces}")
            }
        }
        fields.map(_.toMap) match {
          case Validated.Valid(raw) => raw.asRight
          case Validated.Invalid(errors) =>
            TrackerProtocolViolation(badrows.BadRowPayload.RawEventBadRowPayload(meta, json),
                                     errors.toList.mkString(", "),
                                     "enrich").asLeft
        }
      case None =>
        TrackerProtocolViolation(badrows.BadRowPayload.RawEventBadRowPayload(meta, json),
                                 "Payload is not a JSON object",
                                 "enrich").asLeft
    }
}
