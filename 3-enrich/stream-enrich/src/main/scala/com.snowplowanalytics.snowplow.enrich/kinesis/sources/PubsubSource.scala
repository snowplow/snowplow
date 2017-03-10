/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
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
package com.snowplowanalytics
package snowplow
package enrich
package kinesis
package sources

// Java
import java.net.InetAddress
import java.util.{Properties,UUID}

// Logging
import org.slf4j.LoggerFactory

// Pubsub
import com.google.cloud.pubsub.spi.v1.{SubscriberClient,SubscriberSettings}
import com.google.pubsub.v1.{
  TopicName,
  PubsubMessage,
  SubscriptionName,
  PullRequest,
  ReceivedMessage}

// Scala
import scala.collection.JavaConverters._
import org.joda.time.Duration

// Iglu
import iglu.client.Resolver

// Snowplow events and enrichment
import common.enrichments.EnrichmentRegistry

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Source to read events from a GCP Pub/Sub topic
 */
class PubsubSource(config: KinesisEnrichConfig, igluResolver: Resolver, enrichmentRegistry: EnrichmentRegistry, tracker: Option[Tracker])
    extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {

  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  /**
   * Never-ending processing loop over source stream.
   */
  def run {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    info("Using workerId: " + workerId)

    val subscriber = getTopicSubscriber(config)
    val subscriptionName = SubscriptionName.create(s"${config.projectId}", s"${config.rawInStream}")
    val pullRequest = getPullRequest(config, subscriber)

    info(s"Processing raw input Pubsub topic: ${config.rawInStream}")

    while (true) {
      val acksAndBytes = subscriber.pull(pullRequest)
        .getReceivedMessagesList
        .asScala
        .toList
        .map(getDataAndAckId(_)) // Get the values

      val (recordValues, toAck) = acksAndBytes.unzip

      subscriber.acknowledge(subscriptionName, toAck.asJava)
      enrichAndStoreEvents(recordValues)
    }
  }

  /**
   * Get received message data and ack id
   * @param rcvdMessage received message instance
   * @return tuple with received message bytes and ack id
   */
  private def getDataAndAckId(rcvdMessage: ReceivedMessage): (Array[Byte], String) =
    (rcvdMessage.getMessage.getData.toByteArray, rcvdMessage.getAckId)

  /**
   * Get the a subscriber to the Pub/Sub topic, based on the config file
   * @param config The config file settings
   * @return A topic subscriber instance
   */
  private def getTopicSubscriber(config: KinesisEnrichConfig): SubscriberClient = {
    val settings = createSettings(config)
    SubscriberClient.create(settings) 
  }

  /**
   * Create settings to instantiate the topic subscriber, based on the config file
   * @param config The config file settings
   * @return The topic subscriber settings
   */
  private def createSettings(config: KinesisEnrichConfig): SubscriberSettings = {
    val subSettingsBuilder = SubscriberSettings.defaultBuilder()
    subSettingsBuilder
      .createSubscriptionSettings()
      .getRetrySettingsBuilder()
      .setTotalTimeout(Duration.millis(30000))

    subSettingsBuilder.build
  }
  
  /**
   * Create the pull request object, needed to pull messages from the topic
   * @param config The config file settings
   * @param subscriberClient The topic subscriber instance
   * @return A Pull Request instance
   */
  private def getPullRequest(config: KinesisEnrichConfig, subscriberClient: SubscriberClient): PullRequest = {
    val name = SubscriptionName.create(s"${config.projectId}", s"${config.rawInStream}")
    PullRequest.newBuilder()
      .setSubscriptionWithSubscriptionName(name)
      .setMaxMessages(config.maxRecords) 
      .build
  }

}
