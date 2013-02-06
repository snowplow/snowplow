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
import web.{AttributionEnrichments => AE}
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
      raw.ipAddress.map(ip => e.user_ipaddress = ip)
    }

    // 2. Enrichments which can fail

    // 2a. Failable enrichments which don't need the payload

    val success = 0.success[String]
    val successNel = 0.successNel[String]

    // Attempt to decode the useragent
    val useragent = raw.userAgent match {
      case Some(ua) =>
        val u = CU.decodeString(ua, "useragent", raw.encoding)
        u.flatMap(ua => {
          event.useragent = ua
          ua.success
          })
      case None => success // No fields updated
    }

    // Parse the useragent
    val client = raw.userAgent match {
      case Some(ua) =>
        val ca = CE.extractClientAttributes(ua)
        ca.flatMap(c => {
          event.br_name = c.browserName
          event.br_family = c.browserFamily
          c.browserVersion.map(bv => event.br_version = bv)
          event.br_type = c.browserType
          event.br_renderengine = c.browserType
          event.os_name = c.osName
          event.os_family = c.osName
          event.os_manufacturer = c.osManufacturer
          event.dvce_type = c.deviceType
          event.dvce_ismobile = CU.booleanToByte(c.deviceIsMobile)
          c.success
          })
        ca
      case None => success // No fields updated
    }

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
  
    val transform = event.transform(sourceMap, transformMap)

    // Potentially update the page_url
    val pageUri = PE.extractPageUri(raw.refererUri, Option(event.page_url))
    pageUri.flatMap(uri => {
      event.page_url = uri.toString
      uri.success
      })

    // Marketing attribution
    val campaign = pageUri.fold(
      e => successNel, // No fields updated
      uri => {
        AE.extractMarketingFields(uri, raw.encoding).flatMap(cmp => {
          event.mkt_medium = cmp.medium
          event.mkt_source = cmp.source
          event.mkt_term = cmp.term
          event.mkt_content = cmp.content
          event.mkt_campaign = cmp.campaign
          cmp.success
          })
        })

    // Assemble all of our (potential) errors
    // TODO: there must be some applicative way of doing this without having to disassemble each Validation
    val errors: List[String] =
      transform.fold(
        e => e.toList,
        s => Nil) ++
      campaign.fold(
        e => e.toList,
        s => Nil) ++
      List(useragent, client, pageUri).map(v => v match {
        case Failure(e) => Some(e)
        case Success(a) => None
        }).flatten

    // Do we have errors, or a valid event?
    errors match {
      case h :: t => NonEmptyList(h, t: _*).fail
      case _ => event.success
    }
  }
}