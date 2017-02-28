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
import com.google.cloud.pubsub.spi.v1.Subscriber
import com.google.pubsub.v1.TopicName
import com.google.pubsub.v1.PubsubMessage

// Scala
import scala.collection.JavaConverters._

// Iglu
import iglu.client.Resolver

// Snowplow events and enrichment
import common.enrichments.EnrichmentRegistry

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Source to read events from a Kafka topic
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

    val subscriber = getTopicSubscription(config)
    val pullRequest = getPullRequest(config, subscriber)

    info(s"Processing raw input Pubsub topic: ${config.rawInStream}")

    while (true) {
      val recordValues = pull(pullRequest)
        .getReceivedMessagesList
        .asScala
        .toList
        .map(_.getMessage.getData.toByteArray) // Get the values

      enrichAndStoreEvents(recordValues)
    }
  }

  private def getTopicSubscriber(config: KinesisEnrichConfig): SubscriptionClient = {
    val settings = createSettings(config)
    SubscriberClient.create(settings) 
  }

  private def createSettings(config: KinesisEnrichConfig): Properties = {
    val subSettingsBuilder = SubscriberSettings.newBuilder()
    subSettingsBuilder
      .createSubscriptionSettings()
      .getRetrySettingsBuilder()
      .setTotalTimeout(Duration.millis(30000))

    subSettingsBuilder.build
  }
  
  private def getPullRequest(config: KinesisEnrichConfig, subscriberClient: SubscriptionClient) : PullRequest = {
    val name = SubscriptionName.create(s"${config.projectId}", s"${config.rawInStream}")
    PullRequest.newBuilder()
      .setSubscriptionWithSubscriptionName(name)
      .build()
  }

}
