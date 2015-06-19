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
package com.snowplowanalytics.snowplow.enrich.kinesis

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

  val HeartbeatInterval = 300000L

  /**
   * Configure a Tracker based on the configuration HOCON
   *
   * @param config The "monitoring.snowplow" section of the HOCON
   * @return Tracker instance
   */
  def initializeTracker(config: Config): Tracker = {

    val endpoint = config.getString("collector-uri")

    val port = config.getInt("collector-port")

    val appName = config.getString("app-id")

    // Not yet used
    val method = config.getString("method")

    val emitter = AsyncEmitter.createAndStart(endpoint, port)

    new Tracker(List(emitter), generated.Settings.name, appName)
  }

  /**
   * If a tracker has been configured, send a sink_write_failed event
   *
   * @param tracker
   * @param failureCount the number of consecutive failed writes
   * @param initialFailureTime Time of the first consecutive failed write
   * @param message What went wrong
   */
  def sendFailureEvent(
    tracker: Tracker,
    lastRetryPeriod: Long,
    failureCount: Long,
    initialFailureTime: Long,
    message: String) {

    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/sink_write_failed/jsonschema/1-0-0",
      ("lastRetryPeriod" -> lastRetryPeriod) ~
      ("sink" -> "elasticsearch") ~
      ("failureCount" -> failureCount) ~
      ("initialFailureTime" -> initialFailureTime) ~
      ("message" -> message)
    ))
  }

  /**
   * Send an initialization event and schedule heartbeat and shutdown events
   *
   * @param tracker
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
   * @param tracker
   */
  private def trackApplicationInitialization(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/application_initialized/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send an application_shutdown unstructured event
   *
   * @param tracker
   */
  def trackApplicationShutdown(tracker: Tracker) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/application_shutdown/jsonschema/1-0-0",
      JObject(Nil)
    ))
  }

  /**
   * Send a warning unstructured event
   *
   * @param tracker
   * @param message The warning message
   */
  def trackApplicationWarning(tracker: Tracker, message: String) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/application_warning/jsonschema/1-0-0",
      ("warning" -> message)
    ))
  }

  /**
   * Send a heartbeat unstructured event
   *
   * @param tracker
   * @param heartbeatInterval Time between heartbeats in milliseconds
   */
  private def trackApplicationHeartbeat(tracker: Tracker, heartbeatInterval: Long) {
    tracker.trackUnstructEvent(SelfDescribingJson(
      "iglu:com.snowplowanalytics.snowplow/heartbeat/jsonschema/1-0-0",
      "interval" -> heartbeatInterval
    ))
  }
}
