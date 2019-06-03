/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream

import java.text.SimpleDateFormat

import scala.util.Try

// TODO: file is named Amodel.scala instead of model.scala, rename once >= 2.12.4
// https://github.com/pureconfig/pureconfig/issues/205
object model {

  sealed trait Credentials
  case object NoCredentials extends Credentials
  final case class AWSCredentials(accessKey: String, secretKey: String) extends Credentials

  // Case classes necessary to the decoding of the configuration
  final case class StreamsConfig(
    in: InConfig,
    out: OutConfig,
    sourceSink: SourceSinkConfig,
    buffer: BufferConfig,
    appName: String
  )
  final case class InConfig(raw: String)
  final case class OutConfig(
    enriched: String,
    pii: Option[String],
    bad: String,
    partitionKey: String
  )
  final case class KinesisBackoffPolicyConfig(minBackoff: Long, maxBackoff: Long)
  sealed trait SourceSinkConfig
  final case class Kinesis(
    region: String,
    aws: AWSCredentials,
    maxRecords: Int,
    initialPosition: String,
    initialTimestamp: Option[String],
    backoffPolicy: KinesisBackoffPolicyConfig,
    customEndpoint: Option[String]
  ) extends SourceSinkConfig {
    val timestamp = initialTimestamp
      .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
      .right
      .flatMap { s =>
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
      }
    require(initialPosition != "AT_TIMESTAMP" || timestamp.isRight, timestamp.left.getOrElse(""))

    val streamEndpoint = customEndpoint.getOrElse(region match {
      case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    })
  }
  final case class Kafka(
    brokers: String,
    retries: Int,
    consumerConf: Option[Map[String, String]],
    producerConf: Option[Map[String, String]]
  ) extends SourceSinkConfig
  final case class Nsq(
    rawChannel: String,
    host: String,
    port: Int,
    lookupHost: String,
    lookupPort: Int
  ) extends SourceSinkConfig
  case object Stdin extends SourceSinkConfig
  final case class BufferConfig(
    byteLimit: Long,
    recordLimit: Long,
    timeLimit: Long
  )
  final case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  final case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  final case class RemoteAdapterConfig(
    vendor: String,
    version: String,
    connectionTimeout: Option[Long],
    readTimeout: Option[Long],
    url: String
  )
  final case class EnrichConfig(
    streams: StreamsConfig,
    remoteAdapters: Option[List[RemoteAdapterConfig]],
    monitoring: Option[MonitoringConfig]
  )
}
