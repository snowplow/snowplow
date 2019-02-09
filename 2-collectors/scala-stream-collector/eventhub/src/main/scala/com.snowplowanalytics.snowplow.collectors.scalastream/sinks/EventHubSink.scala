/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream.sinks

import java.util.concurrent.ScheduledExecutorService
import java.util.function.BiFunction

import com.microsoft.azure.eventhubs.{ConnectionStringBuilder, EventData, EventHubClient}
import com.snowplowanalytics.snowplow.collectors.scalastream.model.EventHub
import scalaz._

/** EventHubSink companion object with factory method */
object EventHubSink {

  /**
   * Creates an EventHubSink
   * Exists so that no threads can get a reference to the EventHubSink during its construction
   * TODO: rm scalaz \/ once 2.12
   */
  def createAndInitialize(
                           eventHubConfig: EventHub,
                           streamName: String,
                           domainName: String,
                           sasKeyName: String,
                           sasKey: String,
                           executorService: ScheduledExecutorService
  ): \/[Throwable, EventHubSink] = {

    val connStrBuilder = new ConnectionStringBuilder()
      .setEndpoint( streamName, domainName)
      .setSasKeyName(sasKeyName)
      .setSasKey(sasKey)

    val client = \/.fromTryCatch(EventHubClient.createSync(connStrBuilder.toString, executorService))

    client.map{c =>
      new EventHubSink(c, streamName, executorService)
    }
  }
}

/**
 * Eventhub Sink for the Scala collector.
 */
class EventHubSink private(
  client: EventHubClient,
  streamName: String,
  executorService: ScheduledExecutorService
) extends Sink {

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  implicit lazy val ec = concurrent.ExecutionContext.fromExecutorService(executorService)

  private val biFun = new BiFunction[Void, Throwable, Void] {
    override def apply(v: Void, t: Throwable): Void = {
      if (t != null) sys.error(s"Sending event to EventHub failed: ${t.getMessage}")
      null
    }
  }

  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    log.debug(s"Writing ${events.size} Thrift records to Eventhub $streamName at key $key")
    events.foreach { event =>
      val data = EventData.create(event)
      client.send(data, key).handleAsync(biFun)
    }
    Nil
  }
}
