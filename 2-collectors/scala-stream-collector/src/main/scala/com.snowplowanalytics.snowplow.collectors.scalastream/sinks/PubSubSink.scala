/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.ApiException
import com.google.cloud.pubsub.v1.{Publisher, TopicAdminClient}
import com.google.pubsub.v1.{ProjectName, PubsubMessage, TopicName}
import com.google.protobuf.ByteString
import org.threeten.bp.Duration

import model._

/**
 * PubSub Sink for the Scala collector
 */
class PubSubSink(
  pubSubConfig: PubSub,
  bufferConfig: BufferConfig,
  topicName: String
) extends Sink {

  // maximum size of a pubsub message is 10Mb
  override val MaxBytes: Long = 10000000L

  private val batchingSettings: BatchingSettings =
    BatchingSettings.newBuilder()
      .setElementCountThreshold(bufferConfig.recordLimit)
      .setRequestByteThreshold(bufferConfig.byteLimit)
      .setDelayThreshold(Duration.ofMillis(bufferConfig.timeLimit))
      .build()

  private val retrySettings: RetrySettings =
    RetrySettings.newBuilder()
      .setInitialRetryDelay(Duration.ofMillis(pubSubConfig.backoffPolicy.minBackoff))
      .setMaxRetryDelay(Duration.ofMillis(pubSubConfig.backoffPolicy.maxBackoff))
      .setRetryDelayMultiplier(pubSubConfig.backoffPolicy.multiplier)
      .setTotalTimeout(Duration.ofMillis(pubSubConfig.backoffPolicy.totalBackoff))
      .build()

  private val publisher = createPublisher()
  require(publisher.isRight, s"Couldn't create publisher ${publisher.left.map(_.getMessage)}")
  require(topicExists(topicName), s"PubSub topic $topicName doesn't exist")

  /**
   * Instantiates a Publisher on an existing topic with the given configuration options.
   * This can fail if the publisher can't be created.
   * @return a PubSub publisher or an error
   */
  private def createPublisher(): Either[Throwable, Publisher] =
    try {
      val p = Publisher.newBuilder(TopicName.of(pubSubConfig.googleProjectId, topicName))
        .setBatchingSettings(batchingSettings)
        .setRetrySettings(retrySettings)
        .build()
      Right(p)
    } catch {
      case NonFatal(e) => Left(e)
    }

  private def topicExists(topicName: String): Boolean = (for {
    topicAdminClient <- Try(TopicAdminClient.create()).toOption
    topics <-
      Try(topicAdminClient.listTopics(ProjectName.of(pubSubConfig.googleProjectId)))
        .map(_.iterateAll.asScala.toList)
        .toOption
    exists = topics.map(_.getName).contains(topicName)
  } yield exists).getOrElse(false)

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
    log.info(s"Writing ${events.size} Thrift records to PubSub topic ${topicName}")
    events.foreach { event =>
      publisher.right.map { p =>
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
        })
      }
    }
    Nil
  }
}
