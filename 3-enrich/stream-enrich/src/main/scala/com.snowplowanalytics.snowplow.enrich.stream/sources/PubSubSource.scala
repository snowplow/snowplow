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
package stream
package sources

import scala.util.Try

import com.google.api.core.ApiService.{Listener, State}
import com.google.api.gax.core.InstantiatingExecutorProvider
import com.google.cloud.pubsub.v1._
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1._
import scalaz._
import Scalaz._

import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver
import model.EnrichConfig
import scalatracker.Tracker
import utils._

/** PubSubSource companion object with factory method */
object PubSubSource {
  def createAndInitialize(
    config: EnrichConfig,
    igluResolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): \/[Throwable, PubSubSource] = for {
    pubSubConfig <- config.streams.pubsub.right
    topic = ProjectTopicName.of(pubSubConfig.googleProjectId, config.streams.in.raw)
    subName = ProjectSubscriptionName.of(pubSubConfig.googleProjectId, config.streams.appName)
    _ <- createSubscription(subName, topic)
  } yield new PubSubSource(config, igluResolver, enrichmentRegistry, tracker, subName)

  private def createSubscription(
    sub: ProjectSubscriptionName,
    topic: ProjectTopicName
  ): \/[Throwable, Subscription] = for {
    subscriptionAdminClient <- toEither(Try(SubscriptionAdminClient.create()))
    subscription = subscriptionAdminClient.createSubscription(
      sub, topic, PushConfig.getDefaultInstance(), 0)
    _ <- toEither(Try(subscriptionAdminClient.close()))
  } yield subscription
}

/** Source to read events from a GCP Pub/Sub topic */
class PubSubSource private (
  config: EnrichConfig,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  subName: ProjectSubscriptionName
) extends Source(config, igluResolver, enrichmentRegistry, tracker) {

  override val MaxRecordSize = Some(10000000L)

  private val subscriber = createSubscriber(subName, config.streams.pubsub.threadPoolSize)

  /** Never-ending processing loop over source stream. */
  override def run(): Unit = subscriber.startAsync().awaitRunning()

  /** The subscriber has to be created here because of the receiver. This builder doesn't throw. */
  private def createSubscriber(sub: ProjectSubscriptionName, threadPoolSize: Int): Subscriber = {
    val receiver = new MessageReceiver() {
      override def receiveMessage(msg: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        enrichAndStoreEvents(List(msg.getData().toByteArray()))
        consumer.ack()
      }
    }

    val executorProvider = InstantiatingExecutorProvider.newBuilder()
      .setExecutorThreadCount(threadPoolSize)
      .build()

    val subscriber = {
      val s = Subscriber.newBuilder(sub, receiver)
        .setExecutorProvider(executorProvider)
        .build()
      s.addListener(new Listener() {
        override def failed(from: State, failure: Throwable): Unit =
          log.error("Subscriber is shutting down with state: " + from, failure)
      }, MoreExecutors.directExecutor())
      s
    }
    subscriber
  }
}
