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
import utils.{ConversionUtils => CU}
import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import web.{PageEnrichments => PE}
import utils.DataTransform._

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
    val (dt, tm) = EE.splitDatetime(raw.timestamp)

    // Let's start populating the NonHiveOutput
    // with the fields which cannot error
    val event = new CanonicalOutput().tap { e =>
      e.dt = dt
      e.tm = tm
      e.event_id = EE.generateEventId
      e.v_collector = raw.source.collector
      e.v_etl = ME.etlVersion
      e.user_ipaddress = raw.ipAddress.getOrElse("")
    }

    // 2. Enrichments which can fail

    // Create a list of failed validation messages
    // Yech mutable. This isn't the Scalaz way
    var errors = new ListBuffer[String]

    // 2a. Failable enrichments which don't need the payload

    // Attempt to decode the useragent
    // TODO: invert the boxing, so the Option is innermost, on the Success only.
    val useragent = raw.userAgent.map(CU.decodeString(_, "useragent", raw.encoding))
    useragent.map(_.fold(
      e => errors.append(e),
      s => event.useragent = s))

    // Parse the useragent
    // TODO: invert the boxing, so the Option is innermost, on the Success only.
    val clientAttribs = raw.userAgent.map(CE.extractClientAttributes(_))
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
        event.dvce_ismobile = CU.booleanToByte(s.deviceIsMobile)
      }))

    // 2b. Failable enrichments using the payload

    // Partially apply decodeString to create a TransformFunc
    val decodeString: TransformFunc = CU.decodeString(raw.encoding, _, _)

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    val transformMap: TransformMap =
      Map(("e"       , (EE.extractEventType, "event")),
          ("ip"      , (ME.identity, "user_ipaddress")),
          ("aid"     , (ME.identity, "app_id")),
          ("p"       , (ME.extractPlatform, "platform")),
          ("tid"     , (ME.identity, "txn_id")),
          ("uid"     , (ME.identity, "user_id")),
          ("fp"      , (ME.identity, "user_fingerprint")),
          ("vid"     , (CU.stringToInt, "visit_id")),
          ("tstamp"  , (EE.extractTimestamp, ("dt", "tm"))), // Note tuple target
          ("tv"      , (ME.identity, "tracker_v")),
          ("lang"    , (ME.identity, "br_lang")),
          ("f_pdf"   , (CU.stringToByte, "br_features_pdf")),
          ("f_fla"   , (CU.stringToByte, "br_features_flash")),
          ("f_java"  , (CU.stringToByte, "br_features_java")),
          ("f_dir"   , (CU.stringToByte, "br_features_director")),
          ("f_qt"    , (CU.stringToByte, "br_features_quicktime")),
          ("f_realp" , (CU.stringToByte, "br_features_realplayer")),
          ("f_wma"   , (CU.stringToByte, "br_features_windowsmedia")),
          ("f_gears" , (CU.stringToByte, "br_features_gears")),
          ("f_ag"    , (CU.stringToByte, "br_features_silverlight")),
          ("cookie"  , (CU.stringToByte, "br_cookies")),
          ("res"     , (CE.extractResolution, ("dvce_screenwidth", "dvce_screenheight"))), // Note tuple target
          ("cd"      , (ME.identity, "br_colordepth")),
          ("tz"      , (decodeString, "os_timezone")),
          ("refr"    , (decodeString, "page_referrer")),
          ("url"     , (ME.identity, "page_url")), // Note we may override this below
          ("page"    , (decodeString, "page_title")),
          ("ev_ca"   , (decodeString, "ev_category")),
          ("ev_ac"   , (decodeString, "ev_action")),
          ("ev_la"   , (decodeString, "ev_label")),
          ("ev_pr"   , (decodeString, "ev_property")),
          ("ev_va"   , (decodeString, "ev_value")),
          ("tr_id"   , (decodeString, "tr_orderid")),
          ("tr_af"   , (decodeString, "tr_affiliation")),
          ("tr_tt"   , (decodeString, "tr_total")),
          ("tr_tx"   , (decodeString, "tr_tax")),
          ("tr_sh"   , (decodeString, "tr_shipping")),
          ("tr_ci"   , (decodeString, "tr_city")),
          ("tr_st"   , (decodeString, "tr_state")),
          ("tr_co"   , (decodeString, "tr_country")),
          ("ti_id"   , (decodeString, "ti_orderid")),
          ("ti_sk"   , (decodeString, "ti_sku")),
          ("ti_na"   , (decodeString, "ti_name")),
          ("ti_ca"   , (decodeString, "ti_category")),
          ("ti_pr"   , (decodeString, "ti_price")),
          ("ti_qu"   , (decodeString, "ti_quantity")))

    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap
  
    event.transform(sourceMap, transformMap).fold(
      e => errors.appendAll(e.toList),
      s => Unit)

    // Potentially update the page_url
    PE.extractPageUri(raw.refererUri, Option(event.page_url)).fold(
      e => errors.append(e),
      s => event.page_url = s.toString)

    // Do we have errors, or a valid event?
    errors.toList match {
      case h :: t => NonEmptyList(h, t: _*).fail
      case _ => event.success
    }
  }
}