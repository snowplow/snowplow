/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd.
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
  ): Validation[Throwable, GooglePubSubSource] =
    for {
      googlePubSubConfig <- config.sourceSink match {
        case c: GooglePubSub => c.success
        case _ =>
          new IllegalArgumentException("Configured source/sink is not Google PubSub").failure
      }
      goodPublisher <- GooglePubSubSink
        .validateAndCreatePublisher(googlePubSubConfig, config.buffer, config.out.enriched)
        .validation

      piiPublisher <- (emitPii(enrichmentRegistry), config.out.pii) match {
        case (true, Some(piiStreamName)) =>
          GooglePubSubSink
            .validateAndCreatePublisher(googlePubSubConfig, config.buffer, piiStreamName)
            .validation
            .rightMap(Some(_))
        case (false, Some(piiStreamName)) =>
          new IllegalArgumentException(
            s"PII was configured to not emit, but PII stream name was given as $piiStreamName").failure
        case (true, None) =>
          new IllegalArgumentException(
            "PII was configured to emit, but no PII stream name was given").failure
        case (false, None) => None.success
      }
      badPublisher <- GooglePubSubSink
        .validateAndCreatePublisher(googlePubSubConfig, config.buffer, config.out.bad)
        .validation
      topic = ProjectTopicName.of(googlePubSubConfig.googleProjectId, config.in.raw)
      subName = ProjectSubscriptionName.of(googlePubSubConfig.googleProjectId, config.appName)
      _ <- toEither(createSubscriptionIfNotExist(subName, topic)).validation
    } yield
      new GooglePubSubSource(
        goodPublisher,
        piiPublisher,
        badPublisher,
        igluResolver,
        enrichmentRegistry,
        tracker,
        config,
        googlePubSubConfig,
        subName)

  private def createSubscriptionIfNotExist(
    sub: ProjectSubscriptionName,
    topic: ProjectTopicName
  ): Try[Subscription] =
    for {
      subscriptionAdminClient <- Try(SubscriptionAdminClient.create())
      subscriptions <- Try(
        subscriptionAdminClient.listSubscriptions(ProjectName.of(sub.getProject())))
        .map(_.iterateAll.asScala.toList)
      exists = subscriptions.map(_.getName).exists(_.contains(sub.getSubscription()))
      subscription <- if (exists) {
        Try(subscriptionAdminClient.getSubscription(sub))
      } else {
        // 0 as ackDeadlineS use the default deadline which is 10s
        Try(
          subscriptionAdminClient
            .createSubscription(sub, topic, PushConfig.getDefaultInstance(), 0))
      }
      _ <- Try(subscriptionAdminClient.close())
    } yield subscription
}

/** Source to read events from a GCP Pub/Sub topic */
class GooglePubSubSource private (
  goodPubslisher: Publisher,
  piiPublisher: Option[Publisher],
  badPublisher: Publisher,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  config: StreamsConfig,
  pubsubConfig: GooglePubSub,
  subName: ProjectSubscriptionName
) extends Source(igluResolver, enrichmentRegistry, tracker, config.out.partitionKey) {

  override val MaxRecordSize = Some(10000000L)

  private val subscriber = createSubscriber(subName, pubsubConfig.threadPoolSize)

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new GooglePubSubSink(goodPubslisher, config.out.enriched)
  }
  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = piiPublisher.flatMap {
    somePiiProducer =>
      config.out.pii.map { piiTopic =>
        new ThreadLocal[Sink] {
          override def initialValue: Sink = new GooglePubSubSink(somePiiProducer, piiTopic)
        }
      }
  }
  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new GooglePubSubSink(badPublisher, config.out.bad)
  }

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
      val s = Subscriber
        .newBuilder(sub, receiver)
        .setExecutorProvider(executorProvider)
        .setHeaderProvider(FixedHeaderProvider.create("User-Agent", GooglePubSubEnrich.UserAgent))
        .build()
      s.addListener(
        new Listener() {
          override def failed(from: State, failure: Throwable): Unit =
            log.error("Subscriber is shutting down with state: " + from, failure)
        },
        MoreExecutors.directExecutor()
      )
      s
    }
    subscriber
  }
}
