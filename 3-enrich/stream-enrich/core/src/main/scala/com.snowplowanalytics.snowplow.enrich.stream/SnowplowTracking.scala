/*
 * Copyright (c) 2015-2019 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich.stream

import scala.concurrent.ExecutionContext.Implicits.global

import cats.Id
import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.AsyncEmitter
import io.circe.Json

import model.SnowplowMonitoringConfig
import utils._
import io.circe.JsonObject

/**
 * Functionality for sending Snowplow events for monitoring purposes
 */
object SnowplowTracking {

  private val HeartbeatInterval = 300000L

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring.snowplow" section of the HOCON
   * @return a new tracker instance
   */
  def initializeTracker(config: SnowplowMonitoringConfig): Tracker[Id] = {
    val emitter =
      AsyncEmitter.createAndStart(config.collectorUri, Some(config.collectorPort), false, None)
    new Tracker(NonEmptyList.one(emitter), generated.BuildInfo.name, config.appId)
  }

  /**
   * If a tracker has been configured, send a sink_write_failed event
   *
   * @param tracker a Tracker instance
   * @param errorType the type of error encountered
   * @param errorMessage the error message
   * @param streamName the name of the stream in which
   *        the error occurred
   * @param appName the name of the application
   * @param retryCount the amount of times we have tried
   *        to put to the stream
   * @param putSize the size in bytes of the put request
   */
  def sendFailureEvent(
    tracker: Tracker[Id],
    errorType: String,
    errorMessage: String,
    streamName: String,
    appName: String,
    retryCount: Long,
    putSize: Long
  ): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "stream_write_failed",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(
          ("errorType", Json.fromString(errorType)),
          ("errorMessage", Json.fromString(errorMessage)),
          ("streamName", Json.fromString(streamName)),
          ("appName", Json.fromString(appName)),
          ("retryCount", Json.fromLong(retryCount)),
          ("putSize", Json.fromLong(putSize))
        )
      )
    )

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker[Id]): Unit = {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit =
        trackApplicationShutdown(tracker)
    })

    val heartbeatThread = new Thread {
      override def run(): Unit =
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
    }

    heartbeatThread.start()
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker[Id]): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_initialized",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.fromJsonObject(JsonObject.empty)
      )
    )

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker[Id]): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_shutdown",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.fromJsonObject(JsonObject.empty)
      )
    )

  /**
   * Send a warning unstructured event
   *
   * @param tracker a Tracker instance
   * @param message The warning message
   */
  def trackApplicationWarning(tracker: Tracker[Id], message: String): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_warning",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(("warning", Json.fromString(message)))
      )
    )

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker[Id], heartbeatInterval: Long): Unit =
    tracker.trackSelfDescribingEvent(
      SelfDescribingData[Json](
        SchemaKey(
          "com.snowplowanalytics.monitoring.kinesis",
          "app_heartbeat",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        Json.obj(("internal", Json.fromLong(heartbeatInterval)))
      )
    )
}
