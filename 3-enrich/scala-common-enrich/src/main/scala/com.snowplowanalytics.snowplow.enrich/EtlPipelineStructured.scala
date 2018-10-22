package com.snowplowanalytics.snowplow.enrich

import io.circe.Json
import org.joda.time.DateTime

import cats.data._
import cats.syntax.either._
import cats.syntax.alternative._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.instances.either._
import cats.instances.list._
import cats.instances.option._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.typeclasses.ToData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.client.repositories.{HttpRepositoryRef, RepositoryRefConfig}

import com.snowplowanalytics.snowplow.enrich.badrows.BadRow
import com.snowplowanalytics.snowplow.enrich.badrows.BadRowPayload.{CollectorMeta, RawEvent}
import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}

object EtlPipelineStructured {
  type Error[A]    = Either[BadRow.TrackerProtocolViolation, A]
  type ErrorStr[A] = ValidatedNel[String, A]
  val resolver = Resolver(
    10,
    HttpRepositoryRef(RepositoryRefConfig("Iglu Central", 1, List("com.snowplow")), "http://iglucentral.com", None))
  val enrichmentRegistry = EnrichmentRegistry(Map.empty)
  val loader             = CljTomcatLoader

  /** Transform collector's line into two (possibly empty) lists of bad rows and enriched events */
  def process(registry: EnrichmentRegistry, etlVersion: String, etlTstamp: DateTime, line: String)(
    implicit resolver: Resolver): (List[BadRow], List[EnrichedEvent]) = {
    val result: Either[BadRow.FormatViolation, (List[BadRow], List[EnrichedEvent])] = for {
      // First two steps are equal to EnrichPseudoCode.parseRawPayload
      collectorPayload <- middleware(loader.toCollectorPayload(line), line)
      (trackerViolations, rawEvents)        = splitCollectorPayload(collectorPayload).separate
      (schemaInvalidations, validRawEvents) = rawEvents.map(validate(resolver)).separate
      enrichRow                             = enrich(resolver, registry, etlVersion, etlTstamp)(_)
      (enrichFailures, enrichedEvents)      = validRawEvents.map(enrichRow).separate
    } yield (trackerViolations ++ schemaInvalidations ++ enrichFailures, enrichedEvents)

    result match {
      case Left(formatViolation) => (List(formatViolation), List.empty)
      case Right(badAndGood)     => badAndGood
    }
  }

  /** Confirm that CollectorPayload contains valid POST, should be part of toCollectorPayload */
  def middleware(payload: common.Validated[Option[CollectorPayload]],
                 line: String): Either[BadRow.FormatViolation, Option[CollectorPayload]] = {
    val fixed = payload.fold(e => catsNEL(e).asLeft, _.asRight)
    fixed match {
      case Right(p @ Some(CollectorPayload.TrackerPayload(_, _, _, Some(body), _, _))) =>
        body.toData match {
          case Some(SelfDescribingData(key, _)) if key.vendor == "post_payload" =>
            Right(p)
          case Some(SelfDescribingData(key, _)) =>
            BadRow
              .FormatViolation(badrows.BadRowPayload.RawCollectorBadRowPayload(None, line),
                               s"Unknown schema ${key.toSchemaUri}",
                               "enricher")
              .asLeft
          case None =>
            BadRow
              .FormatViolation(badrows.BadRowPayload.RawCollectorBadRowPayload(None, line),
                               s"Not a valid self-describing payload",
                               "enricher")
              .asLeft
        }
      // GET
      case Right(p @ Some(CollectorPayload.TrackerPayload(api, qs, contentType, None, source, context))) =>
        Right(p)
      // Webhook
      case Right(p @ Some(CollectorPayload.WebhookPayload(api, qs, contentType, body, source, context))) =>
        Right(p)
      case Right(None) =>
        None.asRight
      case Left(errors) =>
        BadRow
          .FormatViolation(badrows.BadRowPayload.RawCollectorBadRowPayload(None, line),
                           errors.toList.mkString(","),
                           "enricher")
          .asLeft
    }

  }

  /** Split single payload into individual events with collector metadata attached
   * Individual events still can have a lot of violations discovered downstream
   */
  def splitCollectorPayload(payload: Option[CollectorPayload]): List[Error[RawEvent]] =
    payload match {
      // POST
      case Some(p @ CollectorPayload.TrackerPayload(api, qs, _, Some(body), source, context)) =>
        val data = body.toData.getOrElse(throw new RuntimeException("middleware")).data
        val events = data.asArray match {
          case Some(jsonArray) =>
            jsonArray.map(extractEvent(_, CollectorMeta.fromPayload(p))).toList
          case None =>
            throw new RuntimeException("middleware")
        }
        Nested(events).map(raw => RawEvent(CollectorMeta(api, source, context), raw)).value
      // GET
      case Some(CollectorPayload.TrackerPayload(api, _, _, None, source, context)) =>
        ???
      // Webhook
      case Some(CollectorPayload.WebhookPayload(api, _, contentType, body, source, context)) =>
        ???
      case None =>
        Nil
    }

  def validate(resolver: Resolver)(rawEvent: RawEvent): Either[BadRow.SchemaInvalidation, RawEvent] = {
    val contexts = rawEvent.event.get("cx").map(decodeJson).traverse(getUnstructEvent(_))
    val unstructEvent =
      rawEvent.event.get("ue_px").map(decodeJson).traverse(getContexts).map(_.sequence.unite)
    val schema = resolver.lookupSchema()

    ???
  }

  def decodeJson(encoded: String) = {
    val decoded = new String(java.util.Base64.getDecoder.decode(encoded))
    val json    = io.circe.jawn.parse(decoded).getOrElse(throw new RuntimeException("Should be valid JSON"))
    json.toData match {
      case Some(data) => data
      case None       => throw new RuntimeException("Should be self-describing JSON already")
    }
  }

  def getUnstructEvent[A: ToData](json: SelfDescribingData[A]) = json.schema match {
    case SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, _, _)) =>
      json.data.toData match {
        case Some(data) => data.asRight
        case None       => "unstruct_event does not contain self-descibing JSON".asLeft
      }
    case other => s"Unknown key $other".asLeft
  }

  def getContexts(contexts: SelfDescribingData[Json]): Either[String, List[SelfDescribingData[Json]]] =
    contexts.schema match {
      case SchemaKey("com.snowplowanalytics.snowplo", "contexts", "jsonschema", SchemaVer.Full(1, _, _)) =>
        contexts.data.asArray match {
          case Some(contexts) =>
            contexts.toList.traverse { ctx =>
              ctx.toData match {
                case Some(data) => Validated.validNel(data)
                case None       => Validated.invalidNel("Not self-describing JSON in contexts")
              }
            } match {
              case Validated.Valid(ctxs)     => ctxs.asRight
              case Validated.Invalid(errors) => errors.toList.mkString(", ").asLeft
            }
          case None => "contexts does not contain array".asLeft
        }
      case other => s"Unknown key $other".asLeft
    }

  def enrich(resolver: Resolver, registry: EnrichmentRegistry, etlVersion: String, etlTstamp: DateTime)(
    event: RawEvent): Either[BadRow.EnrichmentFailure, EnrichedEvent] =
    EnrichmentManager.enrichEvent(registry, etlVersion, etlTstamp, event.toSnowplowRawEvent)(resolver) match {
      case scalaz.Success(e) => e.asRight
      case scalaz.Failure(errors) =>
        BadRow.EnrichmentFailure(event, catsNEL(errors).map(enrichmentError), etlVersion).asLeft
    }

  /** Extract single event from Tracker POST payload */
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
            BadRow
              .TrackerProtocolViolation(badrows.BadRowPayload.RawEventBadRowPayload(meta, json),
                                        errors.toList.mkString(", "),
                                        "enricher")
              .asLeft
        }
      case None =>
        BadRow
          .TrackerProtocolViolation(badrows.BadRowPayload.RawEventBadRowPayload(meta, json),
                                    "Payload is not a JSON object",
                                    "enrich")
          .asLeft
    }

  def enrichmentError(message: String): BadRow.EnrichmentError =
    BadRow.EnrichmentError("unkown", message)

  def catsNEL[A](list: scalaz.NonEmptyList[A]) =
    cats.data.NonEmptyList.fromListUnsafe(list.list)

  implicit class JsonValidation(data: SelfDescribingData[Json]) {
    import com.snowplowanalytics.iglu.client.validation.ValidatableJValue.validate
    def validateWith(resolver: Resolver) = {
      val schema = resolver.lookupSchema(data.schema.toSchemaUri).fold(e => catsNEL(e).asLeft, _.asRight)
      validate(s)

      ???

    }
  }

}
