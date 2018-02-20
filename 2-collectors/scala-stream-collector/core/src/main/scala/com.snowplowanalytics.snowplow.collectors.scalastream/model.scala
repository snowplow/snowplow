/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream

import scala.concurrent.duration.FiniteDuration

import sinks.Sink

package model {

  /**
   * Case class for holding both good and
   * bad sinks for the Stream Collector.
   */
  case class CollectorSinks(good: Sink, bad: Sink)

  /**
   * Case class for holding the results of
   * splitAndSerializePayload.
   *
   * @param good All good results
   * @param bad All bad results
   */
  case class EventSerializeResult(good: List[Array[Byte]], bad: List[Array[Byte]])

  /**
   * Class for the result of splitting a too-large array of events in the body of a POST request
   *
   * @param goodBatches List of batches of events
   * @param failedBigEvents List of events that were too large
   */
  case class SplitBatchResult(goodBatches: List[List[String]], failedBigEvents: List[String])

  final case class CookieConfig(
    enabled: Boolean,
    name: String,
    expiration: FiniteDuration,
    domain: Option[String]
  )
  final case class CookieBounceConfig(
    enabled: Boolean,
    name: String,
    fallbackNetworkUserId: String,
    forwardedProtocolHeader: Option[String]
  )
  final case class RedirectMacroConfig(
    enabled: Boolean,
    placeholder: Option[String]
  )
  final case class P3PConfig(policyRef: String, CP: String)
  final case class CrossDomainConfig(enabled: Boolean, domain: String, secure: Boolean)
  final case class KinesisBackoffPolicyConfig(minBackoff: Long, maxBackoff: Long)
  final case class GooglePubSubBackoffPolicyConfig(
    minBackoff: Long,
    maxBackoff: Long,
    totalBackoff: Long,
    multiplier: Double
  )
  sealed trait SinkConfig
  final case class AWSConfig(accessKey: String, secretKey: String)
  final case class Kinesis(
    region: String,
    threadPoolSize: Int,
    aws: AWSConfig,
    backoffPolicy: KinesisBackoffPolicyConfig
  ) extends SinkConfig {
    val endpoint = region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  final case class GooglePubSub(
    googleProjectId: String,
    backoffPolicy: GooglePubSubBackoffPolicyConfig
  ) extends SinkConfig
  final case class Kafka(brokers: String, retries: Int) extends SinkConfig
  final case class Nsq(host: String, port: Int) extends SinkConfig
  case object Stdout extends SinkConfig
  final case class BufferConfig(byteLimit: Int, recordLimit: Int, timeLimit: Long)
  final case class StreamsConfig(
    good: String,
    bad: String,
    useIpAddressAsPartitionKey: Boolean,
    sink: SinkConfig,
    buffer: BufferConfig
  )
  final case class CollectorConfig(
    interface: String,
    port: Int,
    p3p: P3PConfig,
    crossDomain: CrossDomainConfig,
    cookie: CookieConfig,
    cookieBounce: CookieBounceConfig,
    redirectMacro: RedirectMacroConfig,
    streams: StreamsConfig
  ) {
    val cookieConfig = if (cookie.enabled) Some(cookie) else None

    def cookieName = cookieConfig.map(_.name)
    def cookieDomain = cookieConfig.flatMap(_.domain)
    def cookieExpiration = cookieConfig.map(_.expiration)
  }
}
