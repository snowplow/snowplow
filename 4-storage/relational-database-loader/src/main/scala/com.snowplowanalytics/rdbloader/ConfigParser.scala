/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.rdbloader

import cats.syntax.either._

import io.circe._
import io.circe.generic.auto._
import io.circe.Decoder._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.decoding.ConfiguredDecoder

import Config._

object ConfigParser {

  /**
   * Allow circe codecs decode snake case YAML keys into camel case
   */
  private implicit val decoderConfiguration =
    Configuration.default.withSnakeCaseKeys

  implicit val decodeTrackerMethod: Decoder[TrackerMethod] =
    Decoder.instance(parseTrackerMethod)

  implicit val decodeLoggingLevel: Decoder[LoggingLevel] =
    Decoder.instance(parseLoggingLevel)

  implicit val decodeOutputCompression: Decoder[OutputCompression] =
    Decoder.instance(parseOutputCompression)

  implicit val decodeCollectorFormat: Decoder[CollectorFormat] =
    Decoder.instance(parseCollectorFormat)

  implicit val decodeCollectors: Decoder[Collectors] =
    Decoder.instance(parseCollectors)

  /**
   * Merged all custom decoders with automatic decoders
   */
  private[rdbloader] implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val monitoringDecoder: Decoder[Monitoring] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val enrichVersionsDecoder: Decoder[EnrichVersions] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val enrichDecoder: Decoder[Enrich] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val emrSoftwareDecoder: Decoder[EmrSoftware] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val emrJobflowDecoder: Decoder[EmrJobflow] =
    ConfiguredDecoder.decodeCaseClass

  private[rdbloader] implicit val emrDecoder: Decoder[SnowplowEmr] =
    ConfiguredDecoder.decodeCaseClass


  private def parseCollectors(hCursor: HCursor): Either[DecodingFailure, Collectors] = {
    for {
      jsonObject <- hCursor.as[Map[String, Json]]
      format <- jsonObject.getJson("format", hCursor)
      collectorFormat <- format.as[CollectorFormat]
    } yield Collectors(collectorFormat)
  }

  private def parseCollectorFormat(hCursor: HCursor): Either[DecodingFailure, CollectorFormat] = {
    for {
      string <- hCursor.as[String]
      method  = CollectorFormat.fromString(string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  private def parseTrackerMethod(hCursor: HCursor): Either[DecodingFailure, TrackerMethod] = {
    for {
      string <- hCursor.as[String]
      method  = TrackerMethod.fromString(string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  private def parseLoggingLevel(hCursor: HCursor): Either[DecodingFailure, LoggingLevel] = {
    for {
      string <- hCursor.as[String]
      method  = LoggingLevel.fromString(string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  private def parseOutputCompression(hCursor: HCursor): Either[DecodingFailure, OutputCompression] = {
    for {
      string <- hCursor.as[String]
      method  = OutputCompression.fromString(string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  private implicit class ParseError[A](error: Either[String, A]) {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  private implicit class JsonHash(obj: Map[String, Json]) {
    def getJson(key: String, hCursor: HCursor): Decoder.Result[Json] = obj.get(key) match {
      case Some(success) => Right(success)
      case None => Left(DecodingFailure(s"Key [$key] is missing", hCursor.history))
    }
  }
}
