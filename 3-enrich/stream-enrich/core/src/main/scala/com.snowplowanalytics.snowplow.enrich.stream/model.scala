/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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

object model {

  /**
   * The enrichment process takes input SnowplowRawEvent objects from
   * an input source and outputs enriched objects to a sink,
   * as defined in the following enumerations.
   */
  sealed trait Source
  case object KafkaSource extends Source
  case object KinesisSource extends Source
  case object StdinSource extends Source
  case object NsqSource extends Source

  sealed trait Sink
  case object KafkaSink extends Sink
  case object KinesisSink extends Sink
  case object StdouterrSink extends Sink
  case object NsqSink extends Sink

  /** Whether the sink is for good rows or bad rows */
  sealed trait InputType
  case object Good extends InputType
  case object Bad extends InputType

  sealed trait Credentials
  case object NoCredentials extends Credentials
  final case class AWSCredentials(accessKey: String, secretKey: String) extends Credentials

  // Case classes necessary to the decoding of the configuration
  final case class StreamsConfig(
    in: InConfig,
    out: OutConfig,
    kinesis: KinesisConfig,
    pubsub: PubSubConfig,
    kafka: KafkaConfig,
    nsq: NsqConfig,
    buffer: BufferConfig,
    appName: String
  )
  final case class InConfig(raw: String)
  final case class OutConfig(enriched: String, bad: String, partitionKey: String)
  final case class BackoffPolicyConfig(
    minBackoff: Long,
    maxBackoff: Long,
    totalBackoff: Long,
    multiplier: Double
  )
  final case class KinesisConfig(
    region: String,
    maxRecords: Int,
    initialPosition: String,
    initialTimestamp: Option[String],
    backoffPolicy: BackoffPolicyConfig
  ) {
    val timestamp = initialTimestamp
      .toRight("An initial timestamp needs to be provided when choosing AT_TIMESTAMP")
      .right.flatMap { s =>
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        utils.fold(Try(format.parse(s)))(t => Left(t.getMessage), Right(_))
      }
    require(initialPosition != "AT_TIMESTAMP" || timestamp.isRight, timestamp.left.getOrElse(""))

    val streamEndpoint = region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  final case class PubSubConfig(
    googleProjectId: String,
    backoffPolicy: BackoffPolicyConfig,
    threadPoolSize: Int
  )
  final case class KafkaConfig(brokers: String, retries: Int)
  final case class NsqConfig(
    rawChannel: String,
    host: String,
    port: Int,
    lookupHost: String,
    lookupPort: Int
  )
  final case class BufferConfig(byteLimit: Long, recordLimit: Long, timeLimit: Long)
  final case class MonitoringConfig(snowplow: SnowplowMonitoringConfig)
  final case class SnowplowMonitoringConfig(
    collectorUri: String,
    collectorPort: Int,
    appId: String,
    method: String
  )
  final case class EnrichConfig(
    source: String,
    sink: String,
    aws: AWSCredentials,
    streams: StreamsConfig,
    monitoring: Option[MonitoringConfig]
  ) {
    val sourceType: Source = source.toLowerCase match {
      case "kinesis" => KinesisSource
      case "kafka"   => KafkaSource
      case "stdin"   => StdinSource
      case "nsq"     => NsqSource
      case o         => throw new IllegalArgumentException(s"Unknown enrich.source: $o")
    }
    val sinkType: Sink = sink.toLowerCase match {
      case "kinesis"   => KinesisSink
      case "kafka"     => KafkaSink
      case "stdouterr" => StdouterrSink
      case "nsq"       => NsqSink
      case o           => throw new IllegalArgumentException(s"Unknown enrich.sink: $o")
    }
  }
}