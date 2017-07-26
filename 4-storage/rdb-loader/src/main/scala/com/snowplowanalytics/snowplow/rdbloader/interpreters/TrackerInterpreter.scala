/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package interpreters

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import org.json4s.JObject
import com.snowplowanalytics.snowplow.scalatracker._
import com.snowplowanalytics.snowplow.scalatracker.emitters.{AsyncBatchEmitter, AsyncEmitter}

import scala.util.control.NonFatal

// This project
import config.SnowplowConfig.{Monitoring, GetMethod, PostMethod}

object TrackerInterpreter {

  val ApplicationContextSchema = "iglu:com.snowplowanalytics.monitoring.batch/application_context/jsonschema/1-0-0"
  val LoadSucceededSchema = "iglu:com.snowplowanalytics.monitoring.batch/load_succeeded/jsonschema/1-0-0"
  val LoadFailedSchema = "iglu:com.snowplowanalytics.monitoring.batch/load_failed/jsonschema/1-0-0"

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring config.yml `monitoring` section
   * @return some tracker if enabled, none otherwise
   */
  def initializeTracking(monitoring: Monitoring): Option[Tracker] = {
    monitoring.snowplow.flatMap(_.collector) match {
      case Some(Collector((host, port))) =>
        val emitter = monitoring.snowplow.flatMap(_.method) match {
          case Some(GetMethod) =>
            AsyncEmitter.createAndStart(host, port = port)
          case Some(PostMethod) =>
            AsyncBatchEmitter.createAndStart(host, port = port, bufferSize = 2)
          case None =>
            AsyncEmitter.createAndStart(host, port = port)
        }
        val tracker = new Tracker(List(emitter), "snowplow-rdb-loader", monitoring.snowplow.flatMap(_.appId).getOrElse("rdb-loader"))
        Some(tracker)
      case Some(_) => None
      case None => None
    }
  }

  /**
   * Track error if `tracker` is enabled. Print error otherwise
   *
   * @param tracker some tracker if enabled
   * @param error **sanitized** error message
   */
  def trackError(tracker: Option[Tracker], error: String): Unit = tracker match {
    case Some(t) =>
      t.trackUnstructEvent(SelfDescribingJson(LoadFailedSchema, JObject(Nil)))
    case None => println(error)
  }

  /**
   * Track error if `tracker` is enabled. Do nothing otherwise
   *
   * @param tracker some tracker if enabled
   */
  def trackSuccess(tracker: Option[Tracker]): Unit = tracker match {
    case Some(t) =>
      t.trackUnstructEvent(SelfDescribingJson(LoadSucceededSchema, JObject(Nil)))
    case None => ()
  }

  /**
   * Dump stdout to S3 logging object to be retrieved by EmrEtlRunner later
   *
   * @param s3Client AWS S3 client
   * @param key S3 object, retrieved from EmrEtlRunner
   * @param content plain text to write
   */
  def dumpStdout(s3Client: AmazonS3, key: S3.Key, content: String): Either[String, S3.Key] = {
    try {
      if (S3Interpreter.keyExists(s3Client, key)) {
        Left(s"RDB LOADER ERROR: S3 log object [$key] already exists")
      } else {
        val meta = new ObjectMetadata()
        meta.setContentLength(content.length)
        meta.setContentEncoding("text/plain")

        val (bucket, prefix) = S3.splitS3Key(key)
        val is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        s3Client.putObject(bucket, prefix, is, meta)
        Right(key)
      }

    } catch {
      case NonFatal(e) =>
        Left(e.toString)
    }
  }

  /**
   * Exit job with appropriate status code and printing
   * exit message (same as dumped to S3) to stdout
   *
   * @param result loading result
   * @param dumpResult S3 dumping result
   */
  def exit(result: Log, dumpResult: Either[String, S3.Key]): Int = {
    println(result)
    (result, dumpResult) match {
      case (Log.LoadingSucceeded(_), Right(key)) =>
        println(s"INFO: Logs successfully dumped to S3 [$key]")
        0
      case (Log.LoadingFailed(_, _), Right(key)) =>
        println(s"INFO: Logs successfully dumped to S3 [$key]")
        1
      case (_, Left(error)) =>
        println(s"ERROR: Log-dumping failed: [$error]")
        1
    }
  }

  /**
   * Config helper functions
   */
  private object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true } catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil => Some((host, 80))
        case _ => None
      }
  }
}
