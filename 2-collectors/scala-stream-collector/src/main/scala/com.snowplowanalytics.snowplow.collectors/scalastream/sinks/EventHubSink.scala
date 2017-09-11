/*
 * EventHubSink.cs
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License
 * Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 * Authors: Devesh Shetty
 * Copyright: Copyright (c) 2017 Snowplow Analytics Ltd
 * License: Apache License Version 2.0
 */
package com.snowplowanalytics.snowplow.collectors
package scalastream
package sinks

import java.util.concurrent.CompletableFuture

import com.microsoft.azure.eventhubs.EventHubClient
import com.microsoft.azure.servicebus.ConnectionStringBuilder

class EventHubSink(config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  private val eventHubName = inputType match {
    case InputType.Good => config.eventHubGoodName
    case InputType.Bad  => config.eventHubBadName
  }

  private def createEventHubClient(eventHubName: String): CompletableFuture[EventHubClient] = {
    val connStr = new ConnectionStringBuilder(config.eventHubNamespace, eventHubName,
                                              config.sasKeyName, config.sasKey)
    return EventHubClient.createFromConnectionString(connStr.toString);
  }

  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = ???

  override def getType = Sink.EventHubs
}