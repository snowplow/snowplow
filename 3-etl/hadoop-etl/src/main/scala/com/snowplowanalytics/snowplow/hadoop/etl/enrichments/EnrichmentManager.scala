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
import outputs.CanonicalOutput
import utils.ConversionUtils

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
   * @return a MaybeCanonicalOutput - i.e.
   *         a ValidationNEL containing
   *         either failure Strings or a
   *         NonHiveOutput.
   */
  def enrichEvent(raw: CanonicalInput): ValidatedCanonicalOutput = {

    // Retrieve the payload
    // TODO: add support for other
    // payload types in the future
    val parameters = raw.payload match {
      case NVGetPayload(p) => p
      case _ => throw new FatalEtlException("Only name-value pair GET payloads are currently supported")
    }

    // 1. Enrichments not expected to fail

    // Quick split timestamp into date and time
    val (dt, tm) = EventEnrichments.splitDatetime(raw.timestamp)

    // Let's start populating the NonHiveOutput
    // with the fields which cannot error
    val event = new CanonicalOutput().tap { e =>
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

    // Attempt to decode the useragent
    // TODO: invert the boxing, so the Option is innermost, on the Success only.
    val useragent = raw.userAgent.map(ConversionUtils.decodeString(_, raw.encoding))
    useragent.map(_.fold(
      e => errors.append(e),
      s => event.useragent = s))

    // Parse the useragent
    // TODO: invert the boxing, so the Option is innermost, on the Success only.
    val clientAttribs = raw.userAgent.map(ClientEnrichments.extractClientAttributes(_))
    clientAttribs.map(_.fold(
      e => errors.append(e),
      s => {
        event.br_name = s.browserName
        event.br_family = s.browserFamily
        s.browserVersion.map(bv => event.br_version = bv)
        event.br_type = s.browserType
        event.br_renderengine = s.browserType
        event.os_name = s.osName
        event.os_family = s.osName
        event.os_manufacturer = s.osManufacturer
        event.dvce_type = s.deviceType
        event.dvce_ismobile = ConversionUtils.booleanToByte(s.deviceIsMobile)
      }))

    // 2b. Failable enrichments using the payload

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
          ConversionUtils.stringToInt(value, "Visit ID").fold(
            e => errors.append(e),
            s => event.visit_id = s)
        // Client date and time
        // TODO: we want to move this into separate client_dt, client_tm fields: #149
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
          ConversionUtils.stringToByte(value, "Feature: PDF").fold(
            e => errors.append(e),
            s => event.br_features_pdf = s)
        // Browser has Flash?
        case "f_fla" =>
          ConversionUtils.stringToByte(value, "Feature: Flash").fold(
            e => errors.append(e),
            s => event.br_features_flash = s)
        // Browser has Java?
        case "f_java" =>
          ConversionUtils.stringToByte(value, "Feature: Java").fold(
            e => errors.append(e),
            s => event.br_features_java = s)
        // Browser has Director?
        case "f_dir" =>
          ConversionUtils.stringToByte(value, "Feature: Director").fold(
            e => errors.append(e),
            s => event.br_features_director = s)
        // Browser has Quicktime?
        case "f_qt" =>
          ConversionUtils.stringToByte(value, "Feature: Quicktime").fold(
            e => errors.append(e),
            s => event.br_features_quicktime = s)
        // Browser has RealPlayer?
        case "f_realp" =>
          ConversionUtils.stringToByte(value, "Feature: RealPlayer").fold(
            e => errors.append(e),
            s => event.br_features_realplayer = s)
        // Browser has Windows Media?
        case "f_wma" =>
          ConversionUtils.stringToByte(value, "Feature: Windows Media").fold(
            e => errors.append(e),
            s => event.br_features_windowsmedia = s)
        // Browser has Gears?
        case "f_gears" =>
          ConversionUtils.stringToByte(value, "Feature: Gears").fold(
            e => errors.append(e),
            s => event.br_features_gears = s)
        // Browser has Silverlight?
        case "f_ag" =>
          ConversionUtils.stringToByte(value, "Feature: Silverlight").fold(
            e => errors.append(e),
            s => event.br_features_silverlight = s)

        // TODO: add a warning if unrecognised parameter found when we support warnings
        case _ =>
      }
    })

    // Do we have errors, or a valid event?
    errors.toList match {
      case h :: t => NonEmptyList(h, t: _*).fail
      case _ => event.success
    }
  }
}