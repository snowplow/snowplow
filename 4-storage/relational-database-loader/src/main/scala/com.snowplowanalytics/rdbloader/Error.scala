package com.snowplowanalytics.rdbloader

import io.circe.{ParsingFailure, DecodingFailure}

import com.github.fge.jsonschema.core.report.ProcessingMessage


sealed trait ConfigError
case class ParseError(error: Option[ParsingFailure], message: Option[String]) extends ConfigError
case class DecodingError(decodingFailure: Option[DecodingFailure], message: Option[String]) extends ConfigError
case class ValidationError(processingMessages: List[ProcessingMessage]) extends ConfigError

object ParseError {
  def apply(message: String): ParseError =
    ParseError(None, Some(message))

  def apply(error: ParsingFailure): ParseError =
    ParseError(Some(error), None)
}

object DecodingError {
  def apply(decodingFailure: DecodingFailure): DecodingError =
    DecodingError(Some(decodingFailure), None)

  def apply(message: String): DecodingError =
    DecodingError(None, Some(message))
}

