/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

import io.circe.{ Decoder, Json => Yaml, Error }
import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.decoding.ConfiguredDecoder

import io.circe.yaml.parser

import Utils._

/**
 * Full Snowplow `config.yml` runtime representation
 */
case class Config(
  aws: Config.SnowplowAws,
  collectors: Config.Collectors,
  enrich: Config.Enrich,
  storage: Config.Storage,
  monitoring: Config.Monitoring)


object Config {
  import RefinedTypes._
  import Codecs._

  /**
    * Parse YAML string as `Config` object
    *
    * @param configYml content of `config.yaml`
    * @return either failure with human-readable error or success with `Config`
    */
  def parse(configYml: String): Either[String, Config] = {
    val yaml: Either[Error, Yaml] = parser.parse(configYml)
    yaml.flatMap(_.as[Config]).leftMap(_.toString)  // TODO: make human-readable
  }

  // aws section

  case class SnowplowAws(
    accessKeyId: String,
    secretAccessKey: String,
    s3: SnowplowS3,
    emr: SnowplowEmr)

  // aws.s3 section

  case class SnowplowS3(
    region: String,
    buckets: SnowplowBuckets)

  case class SnowplowBuckets(
    assets: S3Bucket,
    jsonpathAssets: Option[String],
    log: String,
    enriched: EnrichedBucket,
    shredded: ShreddedBucket)

  case class RawBucket(
    in: List[S3Bucket],
    processing: S3Bucket,
    archive: S3Bucket)

  case class EnrichedBucket(
    good: S3Bucket,
    bad: S3Bucket,
    errors: Option[S3Bucket],
    archive: S3Bucket)

  case class ShreddedBucket(
    good: S3Bucket,
    bad: S3Bucket,
    errors: Option[S3Bucket],
    archive: S3Bucket)

  // aws.emr section

  case class SnowplowEmr(
    amiVersion: String,
    region: String,
    jobflowRole: String,
    serviceRole: String,
    placement: Option[String],
    ec2SubnetId: Option[String],
    ec2KeyName: String,
    bootstrap: List[String],
    software: EmrSoftware,
    jobflow: EmrJobflow,
    bootstrapFailureTries: Int,
    additionalInfo: Option[String])   // TODO: JSON

  case class EmrSoftware(
    hbase: Option[String],
    lingual: Option[String])

  case class EmrJobflow(
    masterInstanceType: String,
    coreInstanceCount: Int,
    coreInstanceType: String,
    taskInstanceCount: Int,
    taskInstanceType: String,
    taskInstanceBid: BigDecimal)

  // collectors section

  sealed trait CollectorFormat extends StringEnum
  case object CloudfrontFormat extends CollectorFormat { val asString = "cloudfront" }
  case object ClojureTomcatFormat extends CollectorFormat { val asString = "clj-tomcat" }
  case object ThriftFormat extends CollectorFormat { val asString = "thrift" }
  case object CfAccessLogFormat extends CollectorFormat { val asString = "tsv/com.amazon.aws.cloudfront/wd_access_log" }
  case object UrbanAirshipConnectorFormat extends CollectorFormat { val asString = "ndjson/urbanairship.connect/v1" }

  case class Collectors(format: CollectorFormat)

  // enrich section

  case class Enrich(
    jobName: String,
    versions: EnrichVersions,
    continueOnUnexpectedError: Boolean,
    outputCompression: OutputCompression)

  case class EnrichVersions(
    hadoopEnrich: String,
    hadoopShred: String,
    hadoopElasticsearch: String)

  sealed trait OutputCompression extends StringEnum
  case object NoneCompression extends OutputCompression { val asString = "NONE" }
  case object GzipCompression extends OutputCompression { val asString = "GZIP" }

  // storage section

  case class Storage(download: Download)

  case class Download(folder: Option[String])

  // monitoring section

  case class Monitoring(
    tags: Map[String, String],
    logging: Logging,
    snowplow: SnowplowMonitoring)

  case class Logging(level: LoggingLevel)

  sealed trait LoggingLevel extends StringEnum
  case object DebugLevel extends LoggingLevel { val asString = "DEBUG" }
  case object InfoLevel extends LoggingLevel { val asString = "INFO" }

  case class SnowplowMonitoring(
    method: TrackerMethod,
    appId: String,
    collector: String)  // Host/port pair

  sealed trait TrackerMethod extends StringEnum
  case object GetMethod extends TrackerMethod { val asString = "get" }
  case object PostMethod extends TrackerMethod { val asString = "post" }


  /**
   * Instances of circe `Decoder` type class for all `config.yml` structures
   */
  object Codecs {

    /**
     * Allow circe codecs decode snake case YAML keys into camel case
     * Used by codecs with `ConfiguredDecoder`
     * Codecs should be declared in exact same order (reverse of their appearence in class)
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

    implicit val decodeCollectorFormat: Decoder[CollectorFormat] =
      decodeStringEnum[CollectorFormat]

    implicit val emrJobflowDecoder: Decoder[EmrJobflow] =
      ConfiguredDecoder.decodeCaseClass

    implicit val emrSoftwareDecoder: Decoder[EmrSoftware] =
      ConfiguredDecoder.decodeCaseClass

    implicit val emrDecoder: Decoder[SnowplowEmr] =
      ConfiguredDecoder.decodeCaseClass

    implicit val s3Decoder: Decoder[SnowplowS3] =
      ConfiguredDecoder.decodeCaseClass

    implicit val awsDecoder: Decoder[SnowplowAws] =
      ConfiguredDecoder.decodeCaseClass

    implicit val configDecoder: Decoder[Config] =
      ConfiguredDecoder.decodeCaseClass

  }
}
