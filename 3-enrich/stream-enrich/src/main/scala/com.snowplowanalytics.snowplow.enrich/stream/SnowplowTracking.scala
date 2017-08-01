 /*
 * Copyright (c) 2015 Snowplow Analytics Ltd.
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

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Config
import com.typesafe.config.Config

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.SelfDescribingJson
import com.snowplowanalytics.snowplow.scalatracker.emitters.AsyncEmitter

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
  def initializeTracker(config: Config): Tracker = {
    val endpoint = config.getString("collector-uri")
    val port = config.getInt("collector-port")
    val appName = config.getString("app-id")
    // Not yet used
    val method = config.getString("method")
    val emitter = AsyncEmitter.createAndStart(endpoint, port)
    val tracker = new Tracker(List(emitter), generated.Settings.name, appName)
    tracker.enableEc2Context()
    tracker
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
    tracker: Tracker,
    errorType: String,
    errorMessage: String,
    streamName: String,
    appName: String,
    retryCount: Long,
    putSize: Long) {

    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.monitoring.kinesis/stream_write_failed/jsonschema/1-0-0",
      ("errorType" -> errorType) ~
      ("errorMessage" -> errorMessage) ~
      ("streamName" -> streamName) ~
      ("appName" -> appName) ~
      ("retryCount" -> retryCount) ~
      ("putSize" -> putSize)
    ))
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker a Tracker instance
   */
  def initializeSnowplowTracking(tracker: Tracker) {
    trackApplicationInitialization(tracker)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        trackApplicationShutdown(tracker)
      }
    })

    val heartbeatThread = new Thread {
      override def run() {
        while (true) {
          trackApplicationHeartbeat(tracker, HeartbeatInterval)
          Thread.sleep(HeartbeatInterval)
        }
      }
    }

    heartbeatThread.start()
  }

  /**
   * Send an application_initialized unstructured event
   *
   * @param tracker a Tracker instance
   */
  private def trackApplicationInitialization(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.monitoring.kinesis/app_initialized/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker a Tracker instance
   */
  def trackApplicationShutdown(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.monitoring.kinesis/app_shutdown/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send a warning unstructured event
   *
   * @param tracker a Tracker instance
   * @param message The warning message
   */
  def trackApplicationWarning(tracker: Tracker, message: String) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.monitoring.kinesis/app_warning/jsonschema/1-0-0",
      ("warning" -> message)
    ))
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker a Tracker instance
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker, heartbeatInterval: Long) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.monitoring.kinesis/app_heartbeat/jsonschema/1-0-0",
      "interval" -> heartbeatInterval
    ))
  }
}
