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

import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.api.core.ApiService.{Listener, State}
import com.google.api.gax.core.FixedExecutorProvider
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.pubsub.v1._
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1._
import scalaz._
import Scalaz._

import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver
import model.{GooglePubSub, StreamsConfig}
import scalatracker.Tracker
import sinks.{GooglePubSubSink, Sink}
import utils._

/** GooglePubSubSource companion object with factory method */
object GooglePubSubSource {
  def createAndInitialize(
    config: StreamsConfig,
    igluResolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[Throwable, GooglePubSubSource] = for {
    googlePubSubConfig <- config.sourceSink match {
      case c: GooglePubSub => c.success
      case _ => new IllegalArgumentException("Configured source/sink is not Google PubSub").failure
    }
    goodSink <- GooglePubSubSink
      .createAndInitialize(googlePubSubConfig, config.buffer, config.out.enriched)
      .validation
    threadLocalGoodSink = new ThreadLocal[Sink] {
      override def initialValue = goodSink
    }
    badSink <- GooglePubSubSink
      .createAndInitialize(googlePubSubConfig, config.buffer, config.out.bad)
      .validation
    threadLocalBadSink = new ThreadLocal[Sink] {
      override def initialValue = badSink
    }
    topic = ProjectTopicName.of(googlePubSubConfig.googleProjectId, config.in.raw)
    subName = ProjectSubscriptionName.of(googlePubSubConfig.googleProjectId, config.appName)
    _ <- toEither(createSubscriptionIfNotExist(subName, topic)).validation
  } yield new GooglePubSubSource(threadLocalGoodSink, threadLocalBadSink, igluResolver,
    enrichmentRegistry, tracker, subName, googlePubSubConfig.threadPoolSize, config.out.partitionKey)

  private def createSubscriptionIfNotExist(
    sub: ProjectSubscriptionName,
    topic: ProjectTopicName
  ): Try[Subscription] = for {
    subscriptionAdminClient <- Try(SubscriptionAdminClient.create())
    subscriptions <- Try(subscriptionAdminClient.listSubscriptions(ProjectName.of(sub.getProject())))
      .map(_.iterateAll.asScala.toList)
    exists = subscriptions.map(_.getName).exists(_.contains(sub.getSubscription()))
    subscription <- if (exists) {
      Try(subscriptionAdminClient.getSubscription(sub))
    } else {
      // 0 as ackDeadlineS use the default deadline which is 10s
      Try(subscriptionAdminClient.createSubscription(sub, topic, PushConfig.getDefaultInstance(), 0))
    }
    _ <- Try(subscriptionAdminClient.close())
  } yield subscription
}

/** Source to read events from a GCP Pub/Sub topic */
class GooglePubSubSource private (
  goodSink: ThreadLocal[Sink],
  badSink: ThreadLocal[Sink],
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  subName: ProjectSubscriptionName,
  threadPoolSize: Int,
  partitionKey: String
) extends Source(goodSink, badSink, igluResolver, enrichmentRegistry, tracker, partitionKey) {

  override val MaxRecordSize = Some(10000000L)

  private val subscriber = createSubscriber(subName, threadPoolSize)

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

    val executorProvider = FixedExecutorProvider
      .create(Executors.newScheduledThreadPool(threadPoolSize))

    val subscriber = {
      val s = Subscriber.newBuilder(sub, receiver)
        .setExecutorProvider(executorProvider)
        .setHeaderProvider(FixedHeaderProvider.create("User-Agent", GooglePubSubEnrich.UserAgent))
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
