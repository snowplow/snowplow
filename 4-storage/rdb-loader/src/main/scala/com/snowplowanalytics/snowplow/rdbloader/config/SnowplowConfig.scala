/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package config

import cats.implicits._

import io.circe.{Decoder, Error, Json => Yaml}
import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.decoding.ConfiguredDecoder
import io.circe.yaml.parser

// This project
import S3._
import Semver._
import LoaderError._
import utils.Common._

/**
 * FullDiscovery Snowplow `config.yml` runtime representation
 */
case class SnowplowConfig(
    aws: SnowplowConfig.SnowplowAws,
    enrich: SnowplowConfig.Enrich,
    storage: SnowplowConfig.Storage,
    monitoring: SnowplowConfig.Monitoring)

object SnowplowConfig {
  import Codecs._

  /**
   * Parse YAML string as `SnowplowConfig` object
   *
   * @param configYml content of `config.yaml`
   * @return either failure with human-readable error or success with `SnowplowConfig`
   */
  def parse(configYml: String): Either[ConfigError, SnowplowConfig] = {
    val yaml: Either[Error, Yaml] = parser.parse(configYml)
    yaml.flatMap(_.as[SnowplowConfig]).leftMap(e => DecodingError(e.show))
  }

  // aws section

  case class SnowplowAws(s3: SnowplowS3)

  // aws.s3 section

  case class SnowplowS3(region: String, buckets: SnowplowBuckets)

  case class SnowplowBuckets(
      assets: Folder,
      jsonpathAssets: Option[Folder],
      log: Folder,
      shredded: ShreddedBucket)

  case class ShreddedBucket(
      good: Folder,
      bad: Folder,
      errors: Option[Folder],
      archive: Folder)

  // enrich section

  case class Enrich(
     versions: EnrichVersions,
     outputCompression: OutputCompression)

  case class EnrichVersions(sparkEnrich: Semver)

  sealed trait OutputCompression extends StringEnum
  case object NoneCompression extends OutputCompression { val asString = "NONE" }
  case object GzipCompression extends OutputCompression { val asString = "GZIP" }

  // storage section

  case class Storage(versions: StorageVersions)

  case class StorageVersions(rdbShredder: Semver, hadoopElasticsearch: Semver)

  // monitoring section

  case class Monitoring(
      tags: Map[String, String],
      logging: Logging,
      snowplow: Option[SnowplowMonitoring])

  case class Logging(level: LoggingLevel)

  sealed trait LoggingLevel extends StringEnum
  case object DebugLevel extends LoggingLevel { val asString = "DEBUG" }
  case object InfoLevel extends LoggingLevel { val asString = "INFO" }

  case class SnowplowMonitoring(
      method: Option[TrackerMethod],
      appId: Option[String],
      collector: Option[String])

  sealed trait TrackerMethod extends StringEnum
  case object GetMethod extends TrackerMethod { val asString = "get" }
  case object PostMethod extends TrackerMethod { val asString = "post" }

  object Lens {
    import shapeless._

    val jsonPathsAssets = lens[SnowplowConfig] >> 'aws >> 's3 >> 'buckets >> 'jsonpathAssets

  }

  /**
   * Instances of circe `Decoder` type class for all `config.yml` structures
   */
  object Codecs {

    /**
     * Allow circe codecs decode snake case YAML keys into camel case
     * Used by codecs with `ConfiguredDecoder`
     * Codecs should be declared in exact this order (reverse of their appearence in class)
     */
    private implicit val decoderConfiguration =
      Configuration.default.withSnakeCaseKeys

    implicit val decodeTrackerMethod: Decoder[TrackerMethod] =
      decodeStringEnum[TrackerMethod]

    implicit val decodeLoggingLevel: Decoder[LoggingLevel] =
      decodeStringEnum[LoggingLevel]

    implicit val snowplowMonitoringDecoder: Decoder[SnowplowMonitoring] =
      ConfiguredDecoder.decodeCaseClass

    implicit val monitoringDecoder: Decoder[Monitoring] =
      ConfiguredDecoder.decodeCaseClass

    implicit val decodeOutputCompression: Decoder[OutputCompression] =
      decodeStringEnum[OutputCompression]

    implicit val enrichVersionsDecoder: Decoder[EnrichVersions] =
      ConfiguredDecoder.decodeCaseClass

    implicit val enrichDecoder: Decoder[Enrich] =
      ConfiguredDecoder.decodeCaseClass

    implicit val bucketsDecoder: Decoder[SnowplowBuckets] =
      ConfiguredDecoder.decodeCaseClass

    implicit val s3Decoder: Decoder[SnowplowS3] =
      ConfiguredDecoder.decodeCaseClass

    implicit val awsDecoder: Decoder[SnowplowAws] =
      ConfiguredDecoder.decodeCaseClass

    implicit val storageVersionsDecoder: Decoder[StorageVersions] =
      ConfiguredDecoder.decodeCaseClass

    implicit val storageDecoder: Decoder[Storage] =
      ConfiguredDecoder.decodeCaseClass

    implicit val configDecoder: Decoder[SnowplowConfig] =
      ConfiguredDecoder.decodeCaseClass
  }
}
