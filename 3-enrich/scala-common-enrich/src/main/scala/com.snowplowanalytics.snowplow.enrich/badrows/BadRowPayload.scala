package com.snowplowanalytics.snowplow.enrich.badrows

import java.time.Instant
import java.util.Base64

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload.{
  CollectorApi,
  CollectorContext,
  CollectorSource
}
import io.circe.{Encoder, Json, JsonObject}

sealed trait BadRowPayload {
  def asLine: String = this match {
    case BadRowPayload.RawCollectorBadRowPayload(metadata, rawEvent) =>
      new String(Base64.getEncoder.encode((metadata.asTsv ++ "\t" ++ rawEvent).getBytes))
    case BadRowPayload.RawEvent(metadata, event) =>
      val eventJson = Json.fromJsonObject(JsonObject.fromMap(event.mapValues(Json.fromString))).noSpaces
      new String(Base64.getEncoder.encode((metadata.asTsv ++ "\t" ++ eventJson).getBytes))
    case BadRowPayload.Enriched(data) =>
      data
  }
}

object BadRowPayload {

  /** Taken from Scala Common Enrich */
  final case class CollectorMeta(api: CollectorApi, source: CollectorSource, context: CollectorContext) {
    def asTsv: String =
      List(
        api.vendor,
        api.version,
        source.encoding,
        source.hostname.getOrElse(""),
        source.name,
        context.userId.getOrElse(""),
        context.headers.mkString("\t")
      ).mkString("\t")
  }

  object CollectorMeta {
    def fromPayload(payload: CollectorPayload): CollectorMeta =
      CollectorMeta(payload.api, payload.source, payload.context)
  }

  // RawPayload -> SingleEvent ->

  /** Possibly completely invalid payload, anything passed by collector (POST-TSV/GET/anything) */
  case class RawCollectorBadRowPayload(metadata: CollectorMeta, rawEvent: String) extends BadRowPayload

  /** Single *invalid* entity that couldn't be transformed to `SingleEvent`
   * Intermediate format, exists only for bad rows */
  case class RawEventBadRowPayload(metadata: CollectorMeta, payload: Json) extends BadRowPayload

  /** Valid Snowplow event ready for enrichment */
  case class RawEvent(metadata: CollectorMeta, event: Map[String, String]) extends BadRowPayload

  /** Post enrichment failure (JSON or TSV) */
  case class Enriched(data: String) extends BadRowPayload

  implicit val payloadEncoder: Encoder[BadRowPayload] =
    Encoder.instance(payload => Json.fromString(payload.asLine))
}
