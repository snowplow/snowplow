package com.snowplowanalytics.snowplow.enrich
package badrows

import java.util.Base64

import common.adapters.{RawEvent => SnowplowRawEvent}

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload.{
  CollectorApi,
  CollectorContext,
  CollectorSource
}
import io.circe.{Encoder, Json, JsonObject}

sealed trait BadRowPayload {
  def asLine: String = this match {
    case BadRowPayload.RawCollectorBadRowPayload(_, rawEvent) =>
      new String(Base64.getEncoder.encode(rawEvent.getBytes))
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

  /** Possibly completely invalid payload, anything passed by collector (POST-TSV/GET/anything)
   * In case `metadata` is None, it is even not a collector's line
   */
  case class RawCollectorBadRowPayload(metadata: Option[CollectorMeta], rawEvent: String) extends BadRowPayload

  /** Single *invalid* entity that couldn't be transformed to `SingleEvent`
   * Intermediate format, exists only for bad rows */
  case class RawEventBadRowPayload(metadata: CollectorMeta, payload: Json) extends BadRowPayload

  /** Valid Snowplow event ready for enrichment */
  case class RawEvent(meta: CollectorMeta, event: Map[String, String]) extends BadRowPayload {
    /* Isomorphic */
    def toSnowplowRawEvent: SnowplowRawEvent =
      SnowplowRawEvent(meta.api, event, None, meta.source, meta.context)
  }

  /** Post enrichment failure (JSON or TSV) */
  case class Enriched(data: String) extends BadRowPayload

  implicit val payloadEncoder: Encoder[BadRowPayload] =
    Encoder.instance(payload => Json.fromString(payload.asLine))
}
