/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments

// Scala
import scala.collection.mutable.ListBuffer

// Scalaz
import scalaz._
import Scalaz._

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// This project
import inputs.{CanonicalInput, NVGetPayload}
import outputs.NonHiveOutput

/**
 * A module to hold our enrichment process.
 *
 * At the moment this is very fixed - no
 * support for configuring enrichments etc.
 */
object EnrichmentManager {

  /**
   * Runs our enrichment process.
   *
   * @param input Our canonical input
   *        to enrich
   * @return a MaybeNonHiveOutput - i.e.
   *         a ValidationNEL containing
   *         either failure Strings or a
   *         NonHiveOutput.
   */
  def enrichEvent(raw: CanonicalInput): MaybeNonHiveOutput = {

    // 1. Enrichments not expected to fail

    // Quick split timestamp into date and time
    val (dt, tm) = EventEnrichments.splitDatetime(raw.timestamp)

    // Let's start populating the NonHiveOutput
    // with the fields which cannot error
    val event = new NonHiveOutput().tap { e =>
      e.dt = dt
      e.tm = tm
      e.event_id = EventEnrichments.generateEventId
      e.v_collector = raw.source.collector
      e.v_etl = MiscEnrichments.etlVersion
      e.user_ipaddress = raw.ipAddress.getOrElse("")
    }

    // 2. Enrichments which can fail

    // Create a list of failed validation messages
    // Yech mutable. This isn't the Scalaz way
    var errors = new ListBuffer[String]

    // 2a. Failable enrichments which don't need the payload

    // Useragent analysis TODO

    // 2b. Failable enrichments using the payload

    // Retrieve the payload
    // TODO: add support for other
    // payload types in the future
    val parameters = raw.payload match {
      case NVGetPayload(p) => p
      case _ => throw new Exception("OH MY GOD")
    }

    // We copy the Hive ETL approach: one
    // big loop through all the NV pairs
    // present, populating as we go.
    // TODO: in the Avro future we will be
    // more strict and check that a raw row
    // maps onto a specific event type and
    // the required fields for that event
    // type are present
    parameters.foreach(p => {
      val name = p.getName
      val value = p.getValue

      name match {
        // Event type
        case "e" =>
          EventEnrichments.extractEventType(value).fold(
            e => errors.append(e),
            s => event.event = s)
        // IP address override
        case "ip" => event.user_ipaddress = value
        // Application/site ID
        case "aid" => event.app_id = value
        // Platform
        case "p" =>
          MiscEnrichments.extractPlatform(value).fold(
            e => errors.append(e),
            s => event.platform = s)
        // Transaction ID
        case "tid" => event.txn_id = value
        // User ID
        case "uid" => event.user_id = value
        // User fingerprint
        case "fp" => event.user_fingerprint = value
        // Visit ID
        case "vid" =>
          /* MiscEnrichments.extractInt(value, "Visit ID").fold(
            e => errors.append(e),
            s => event.visit_id = s) */
        // Client date and time
        // TODO: we want to move this into separate client dt, tm fields: #149
        case "tstamp" =>
          EventEnrichments.extractTimestamp(value).fold(
            e => errors.append(e),
            s => {
              event.dt = s._1
              event.tm = s._2
            })
        // Tracker version
        case "tv" => event.v_tracker = value
        // Browser language
        case "lang" => event.br_lang = value
        // Browser has PDF?
        case "f_pdf" =>
          /* MiscEnrichments.extractByte(value, "Visit ID").fold(
            e => errors.append(e),
            s => event.br_features_pdf = s) */

        // TODO: add a warning if unrecognised parameter found
      }
    })

    // Do we have errors, or a valid event?
    errors.toList match {
      case h :: t => NonEmptyList(h, t: _*).fail
      case _ => event.success
    }
  }
}