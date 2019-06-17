/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream
package sinks

import java.util.concurrent.Executors

import scala.collection.JavaConverters._

import cats.syntax.either._
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.{ApiException, FixedHeaderProvider}
import com.google.cloud.pubsub.v1.{Publisher, TopicAdminClient}
import com.google.pubsub.v1.{ProjectName, PubsubMessage, ProjectTopicName}
import com.google.protobuf.ByteString
import org.threeten.bp.Duration

import model._
import util._

/** GooglePubSubSink companion object with factory method */
object GooglePubSubSink {
  def createAndInitialize(
    googlePubSubConfig: GooglePubSub,
    bufferConfig: BufferConfig,
    topicName: String
  ): Either[Throwable, GooglePubSubSink] = for {
    batching <- batchingSettings(bufferConfig).asRight
    retry = retrySettings(googlePubSubConfig.backoffPolicy)
    publisher <-
      createPublisher(googlePubSubConfig.googleProjectId, topicName, batching, retry)
    _ <- topicExists(googlePubSubConfig.googleProjectId, topicName)
      .flatMap { b =>
        if (b) b.asRight
        else new IllegalArgumentException(s"Google PubSub topic $topicName doesn't exist").asLeft
      }
  } yield new GooglePubSubSink(publisher, topicName)

  private val UserAgent = s"snowplow/stream-enrich-${generated.BuildInfo.version}"

  /**
   * Instantiates a Publisher on an existing topic with the given configuration options.
   * This can fail if the publisher can't be created.
   * @return a PubSub publisher or an error
   */
  private def createPublisher(
    projectId: String,
    topicName: String,
    batchingSettings: BatchingSettings,
    retrySettings: RetrySettings
  ): Either[Throwable, Publisher] =
    Either.catchNonFatal(Publisher.newBuilder(ProjectTopicName.of(projectId, topicName))
      .setBatchingSettings(batchingSettings)
      .setRetrySettings(retrySettings)
      .setHeaderProvider(FixedHeaderProvider.create("User-Agent", UserAgent))
      .build())

  private def batchingSettings(bufferConfig: BufferConfig): BatchingSettings =
    BatchingSettings.newBuilder()
      .setElementCountThreshold(bufferConfig.recordLimit)
      .setRequestByteThreshold(bufferConfig.byteLimit)
      .setDelayThreshold(Duration.ofMillis(bufferConfig.timeLimit))
      .build()

  /** Defaults are used for the rpc configuration, see Publisher.java */
  private def retrySettings(backoffPolicy: GooglePubSubBackoffPolicyConfig): RetrySettings =
    RetrySettings.newBuilder()
      .setInitialRetryDelay(Duration.ofMillis(backoffPolicy.minBackoff))
      .setMaxRetryDelay(Duration.ofMillis(backoffPolicy.maxBackoff))
      .setRetryDelayMultiplier(backoffPolicy.multiplier)
      .setTotalTimeout(Duration.ofMillis(backoffPolicy.totalBackoff))
      .setInitialRpcTimeout(Duration.ofSeconds(10))
      .setRpcTimeoutMultiplier(2)
      .setMaxRpcTimeout(Duration.ofSeconds(10))
      .build()

  /** Checks that a PubSub topic exists **/
  private def topicExists(projectId: String, topicName: String): Either[Throwable, Boolean] = for {
    topicAdminClient <- Either.catchNonFatal(TopicAdminClient.create())
    topics <- Either.catchNonFatal(topicAdminClient.listTopics(ProjectName.of(projectId)))
      .map(_.iterateAll.asScala.toList)
    exists = topics.map(_.getName).exists(_.contains(topicName))
    _ <- Either.catchNonFatal(topicAdminClient.close())
  } yield exists
}

/**
 * Google PubSub Sink for the Scala collector
 */
class GooglePubSubSink private (publisher: Publisher, topicName: String) extends Sink {

  // maximum size of a pubsub message is 10MB
  override val MaxBytes: Int = 10000000

  /**
   * Convert event bytes to a PubsubMessage to be published
   * @param event Event to be converted
   * @return a PubsubMessage
   */
  private def eventToPubsubMessage(event: Array[Byte]): PubsubMessage =
    PubsubMessage.newBuilder
      .setData(ByteString.copyFrom(event))
      .build()

  /**
   * Store raw events in the PubSub topic
   * @param events The list of events to send
   * @param key Not used.
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    if (events.nonEmpty)
      log.debug(s"Writing ${events.size} Thrift records to Google PubSub topic ${topicName}")
    events.foreach { event =>
      publisher.asRight.map { p =>
        val future = p.publish(eventToPubsubMessage(event))
        ApiFutures.addCallback(future, new ApiFutureCallback[String]() {
          override def onSuccess(messageId: String): Unit =
            log.debug(s"Successfully published event with id $messageId to $topicName")
          override def onFailure(throwable: Throwable): Unit = throwable match {
            case apiEx: ApiException => log.error(
              s"Publishing message to $topicName failed with code ${apiEx.getStatusCode}: " +
                apiEx.getMessage)
            case t => log.error(s"Publishing message to $topicName failed with ${t.getMessage}")
          }
        }, Executors.newSingleThreadExecutor())
      }
    }
    Nil
  }
}
