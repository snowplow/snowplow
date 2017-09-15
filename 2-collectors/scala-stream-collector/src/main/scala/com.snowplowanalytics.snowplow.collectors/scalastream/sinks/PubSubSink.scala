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
package com.snowplowanalytics.snowplow.collectors
package scalastream
package sinks

// Java
import java.util.Properties
import java.io.FileInputStream

// Scala
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._

// PubSub
import com.google.cloud.pubsub.v1.{Publisher, TopicAdminClient, SubscriptionAdminClient}
import com.google.pubsub.v1.{TopicName, Topic, PubsubMessage, ListTopicSubscriptionsRequest}
import com.google.protobuf.ByteString
import io.grpc.{Status, StatusRuntimeException}
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;

// Batching and Retries
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.batching.BatchingSettings
import org.threeten.bp.Duration


/**
 * PubSub Sink for the Scala collector
 */
class PubSubSink(config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  import log.{error, debug, info, trace, warn}

  // not used, but required for this class to implement AbstractSink
  val MaxBytes = 1000000L

  val ByteThreshold = config.byteLimit
  val RecordThreshold = config.recordLimit
  val TimeThreshold = config.timeLimit

  val maxBackoff = config.pubsubMaxBackoff
  val minBackoff = config.pubsubMinBackoff
  val retryDelayMultiplier = config.retryDelayMultiplier
  val totalTimeOut = config.totalTimeOut
  val initialRpcTimeout = config.initialRpcTimeout
  val maxRpcTimeout = config.maxRpcTimeout

  private val topicName = inputType match {
    case InputType.Good => config.pubsubTopicGoodName
    case InputType.Bad  => config.pubsubTopicBadName
  }

  val topic = TopicName.create(config.googleProjectId, topicName)

  private val batchingSettings: BatchingSettings =
    BatchingSettings.newBuilder()
      .setElementCountThreshold(RecordThreshold)
      .setRequestByteThreshold(ByteThreshold)
      .setDelayThreshold(Duration.ofMillis(TimeThreshold))
      .build

  private val retrySettings: RetrySettings =
    RetrySettings.newBuilder()
      .setMaxRetryDelay(Duration.ofMillis(maxBackoff))
      .setInitialRetryDelay(Duration.ofMillis(minBackoff))
      .setRetryDelayMultiplier(retryDelayMultiplier)
      .setTotalTimeout(Duration.ofMillis(totalTimeOut))
      .setInitialRpcTimeout(Duration.ofMillis(initialRpcTimeout))
      .setMaxRpcTimeout(Duration.ofMillis(maxRpcTimeout))
      .build

  private val pubSubPublisher = createPublisher

  /**
   * Checks if a given pubsub topic exists
   *
   * @return Boolean
   */
  private def topicExists(topic: TopicName): Boolean = {
    val topicAdminClient = TopicAdminClient.create()
    try {
      val response = topicAdminClient.getTopic(topic)
      true
    } catch {
      case e: com.google.api.gax.grpc.GrpcApiException => false
    }
  }

  /**
   * Checks if a topic has >0 subscriptions attached
   *
   * @return Boolean
   */
  private def hasSubscriptions(topic: TopicName): Boolean = {
    val subscriptionAdminClient = SubscriptionAdminClient.create()
    val topicAdminClient = TopicAdminClient.create()
    val topicSubscriptionsRequest = ListTopicSubscriptionsRequest.newBuilder()
      .setTopicWithTopicName(topic)
      .build()

    val response = topicAdminClient.listTopicSubscriptions(topicSubscriptionsRequest);

    val subscriptions = response.iterateAll()
    subscriptions.size > 0
  }

  /**
   * Instantiates a Publisher on an existing topic
   * with the given configuration options. If the name isn't correct, this will fail
   *
   * @return a PubSub publisher
   */
  private def createPublisher: Publisher = {
    val doesTopicExist = topicExists(topic)
    if (doesTopicExist == false) {
      throw new RuntimeException(s"The pubsub topic $topicName was not found.")
    }
    val topicHasSubscriptions = hasSubscriptions(topic)
    if (topicHasSubscriptions == false) {
      warn(s"The topic $topicName has no associated subscriptions")
    }
    val publisher = Publisher.defaultBuilder(
        topic
      )
      .setBatchingSettings(batchingSettings)
      .setRetrySettings(retrySettings)
      .build
    publisher
  }

  /**
   * Convert event bytes to PubsubMessage to be published
   * @param event Event to be converted
   * @return PubsubMessage instance
   */
  private def eventToPubsubMessage(event: Array[Byte]): PubsubMessage =
    PubsubMessage.newBuilder
      .setData(ByteString.copyFrom(event))
      .build

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key Not used.
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    debug(s"Writing ${events.size} Thrift records to PubSub topic ${topicName}")
    events.foreach {
      event =>
        val messageIdFuture = pubSubPublisher.publish(eventToPubsubMessage(event))

        ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback[String]() {
          override def onSuccess(messageId: String): Unit = {
            info(s"Published event with messageId: $messageId")
          }
          override def onFailure(throwable: Throwable): Unit = {
            error("Failed to publish", throwable)
          }
        })
      }
    Nil
  }

  override def getType = Sink.PubSub
}