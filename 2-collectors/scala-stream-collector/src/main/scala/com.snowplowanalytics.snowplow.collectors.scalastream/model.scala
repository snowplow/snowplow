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

import com.amazonaws.auth._

import sinks._

package model {

  /** Type of sink */
  sealed trait SinkType
  case object Kinesis extends SinkType
  case object Kafka extends SinkType
  case object Stdout extends SinkType

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
    fallbackNetworkUserId: String
  )
  final case class P3PConfig(policyRef: String, CP: String)
  final case class AWSConfig(accessKey: String, secretKey: String) {
    val provider = ((accessKey, secretKey) match {
      case (a, s) if isDefault(a) && isDefault(s) =>
        Right(new DefaultAWSCredentialsProviderChain())
      case (a, s) if isDefault(a) || isDefault(s) =>
        Left("accessKey and secretKey must both be set to 'default' or neither")
      case (a, s) if isIam(a) && isIam(s) =>
        Right(InstanceProfileCredentialsProvider.getInstance())
      case (a, s) if isIam(a) && isIam(s) =>
        Left("accessKey and secretKey must both be set to 'iam' or neither")
      case (a, s) if isEnv(a) && isEnv(s) =>
        Right(new EnvironmentVariableCredentialsProvider())
      case (a, s) if isEnv(a) || isEnv(s) =>
        Left("accessKey and secretKey must both be set to 'env' or neither")
      case _ =>
        Right(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
    }).fold(s => throw new IllegalArgumentException(s), identity)

    private def isDefault(key: String): Boolean = key == "default"
    private def isIam(key: String): Boolean = key == "iam"
    private def isEnv(key: String): Boolean = key == "env"
  }
  final case class BackoffPolicyConfig(minBackoff: Long, maxBackoff: Long)
  final case class KinesisConfig(
    region: String,
    threadPoolSize: Int,
    aws: AWSConfig,
    backoffPolicy: BackoffPolicyConfig
  ) {
    val endpoint = region match {
      case cn@"cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    }
  }
  final case class KafkaConfig(brokers: String, retries: Int)
  final case class BufferConfig(byteLimit: Int, recordLimit: Int, timeLimit: Long)
  final case class StreamsConfig(
    good: String,
    bad: String,
    useIpAddressAsPartitionKey: Boolean,
    kinesis: KinesisConfig,
    kafka: KafkaConfig,
    buffer: BufferConfig
  )
  final case class CollectorConfig(
    interface: String,
    port: Int,
    p3p: P3PConfig,
    cookie: CookieConfig,
    cookieBounce: CookieBounceConfig,
    sink: String,
    streams: StreamsConfig
  ) {
    val cookieConfig = if (cookie.enabled) Some(cookie) else None

    def cookieName = cookieConfig.map(_.name)
    def cookieDomain = cookieConfig.flatMap(_.domain)
    def cookieExpiration = cookieConfig.map(_.expiration)

    val sinkType = sink match {
      case "kinesis" => Kinesis
      case "kafka"   => Kafka
      case "stdout"  => Stdout
      case o         => throw new IllegalArgumentException(s"collector.sink unknown: $o")
    }
  }
}
