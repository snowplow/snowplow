package com.snowplowanalytics.snowplow.enrich.badrows

import io.circe.{Encoder, Json}
import io.circe.syntax._

// TODO: to iglu core
sealed trait IgluParseError

object IgluParseError {

  /** Payload does not conform Iglu format (e.g. no `data` or `schema` */
  case class InvalidPayload(original: Json) extends IgluParseError

  /** Not a valid iglu URI in `schema` (e.g. missing `iglu:` protocol) */
  case class InvalidUri(schema: String) extends IgluParseError

  implicit val encoder: Encoder[IgluParseError] = Encoder.instance {
    case InvalidPayload(json) =>
      Json.fromFields(List("error" -> "INVALID_PAYLOAD".asJson, "original" -> json))
    case InvalidUri(uri) =>
      Json.fromFields(List("error" -> "INVALID_URI".asJson, "schema" -> Json.fromString(uri)))
  }
}
