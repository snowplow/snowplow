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

  import OutputCompression._
  import Collectors._
  import LoggingLevel._
  import TrackerMethod._
  import SnowplowMonitoring._

  /**
   * Allow circe codecs decode snake case YAML keys into camel case
   */
  private implicit val decoderConfiguration =
    Configuration.default.withSnakeCaseKeys

  /**
   * Merged all custom decoders with automatic decoders
   */
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

  private [rdbloader] implicit val s3Decoder: Decoder[SnowplowS3] =
    ConfiguredDecoder.decodeCaseClass

  private [rdbloader] implicit val awsDecoder: Decoder[SnowplowAws] =
    ConfiguredDecoder.decodeCaseClass

  private [rdbloader] implicit val configDecoder: Decoder[Config] =
    ConfiguredDecoder.decodeCaseClass

  implicit class ParseError[A](error: Either[String, A]) {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  implicit class JsonHash(obj: Map[String, Json]) {
    def getJson(key: String, hCursor: HCursor): Decoder.Result[Json] = obj.get(key) match {
      case Some(success) => Right(success)
      case None => Left(DecodingFailure(s"Key [$key] is missing", hCursor.history))
    }
  }
}
