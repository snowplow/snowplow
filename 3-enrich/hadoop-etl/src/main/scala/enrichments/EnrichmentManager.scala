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
package com.snowplowanalytics.snowplow.enrich.hadoop
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
import utils.MapTransformer._

import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import web.{PageEnrichments => PE}
import web.{AttributionEnrichments => AE}

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
   *         a ValidationNel containing
   *         either failure Strings or a
   *         NonHiveOutput.
   */
  def enrichEvent(raw: CanonicalInput): ValidatedCanonicalOutput = {

    // Placeholders for where the Success value doesn't matter.
    // Useful when you're updating large (>22 field) POSOs.
    val unitSuccess = ().success[String]
    val unitSuccessNel = ().successNel[String]

    // Retrieve the payload
    // TODO: add support for other
    // payload types in the future
    val parameters = raw.payload match {
      case NVGetPayload(p) => p
      case _ => throw new FatalEtlError("Only name-value pair GET payloads are currently supported") // TODO: change back to FatalEtlException when Cascading FailureTrap supports exclusions
    }

    // 1. Enrichments not expected to fail

    // Let's start populating the CanonicalOutput
    // with the fields which cannot error
    val event = new CanonicalOutput().tap { e =>
      e.collector_tstamp = EE.toTimestamp(raw.timestamp)
      e.event_id = EE.generateEventId
      e.event_vendor = "com.snowplowanalytics" // TODO: this should be moved to Tracker Protocol
      e.v_collector = raw.source.collector
      e.v_etl = ME.etlVersion
      raw.ipAddress.map(ip => e.user_ipaddress = ip)
    }

    // 2. Enrichments which can fail

    // 2a. Failable enrichments which don't need the payload

    // Attempt to decode the useragent
    val useragent = raw.userAgent match {
      case Some(ua) =>
        val u = CU.decodeString(raw.encoding, "useragent", ua)
        u.flatMap(ua => {
          event.useragent = ua
          ua.success
          })
      case None => unitSuccess // No fields updated
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
          event.br_renderengine = c.browserRenderEngine
          event.os_name = c.osName
          event.os_family = c.osFamily
          event.os_manufacturer = c.osManufacturer
          event.dvce_type = c.deviceType
          event.dvce_ismobile = CU.booleanToByte(c.deviceIsMobile)
          c.success
          })
        ca
      case None => unitSuccess // No fields updated
    }

    // 2b. Failable enrichments using the payload

    // Partially apply decodeString to create a TransformFunc
    val decodeString: TransformFunc = CU.decodeString(raw.encoding, _, _)

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    // Caution: by definition, a TransformMap loses type safety. Always unit test!
    val transformMap: TransformMap =
      Map(("e"       , (EE.extractEventType, "event")),
          ("ip"      , (ME.identity, "user_ipaddress")),
          ("aid"     , (ME.identity, "app_id")),
          ("p"       , (ME.extractPlatform, "platform")),
          ("tid"     , (ME.identity, "txn_id")),
          ("uid"     , (ME.identity, "user_id")),
          ("duid"    , (ME.identity, "domain_userid")),
          ("nuid"    , (ME.identity, "network_userid")),
          ("fp"      , (ME.identity, "user_fingerprint")),
          ("vid"     , (CU.stringToJInteger, "domain_sessionidx")),
          ("dtm"     , (EE.extractTimestamp, "dvce_tstamp")),
          ("tv"      , (ME.identity, "v_tracker")),
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
          ("url"     , (decodeString, "page_url")), // Note we may override this below
          ("page"    , (decodeString, "page_title")),
          ("cs"      , (ME.identity, "doc_charset")),
          ("ds"      , (CE.extractResolution, ("doc_width", "doc_height"))),
          ("vp"      , (CE.extractResolution, ("br_viewwidth", "br_viewheight"))),
          // Custom structured events
          ("ev_ca"   , (decodeString, "ev_category")),   // LEGACY tracker var. TODO: Remove in late 2013
          ("ev_ac"   , (decodeString, "ev_action")),     // LEGACY tracker var. TODO: Remove in late 2013
          ("ev_la"   , (decodeString, "ev_label")),      // LEGACY tracker var. TODO: Remove in late 2013
          ("ev_pr"   , (decodeString, "ev_property")),   // LEGACY tracker var. TODO: Remove in late 2013
          ("ev_va"   , (CU.stringToJFloat, "ev_value")), // LEGACY tracker var. TODO: Remove in late 2013
          ("se_ca"   , (decodeString, "ev_category")),
          ("se_ac"   , (decodeString, "ev_action")),
          ("se_la"   , (decodeString, "ev_label")),
          ("se_pr"   , (decodeString, "ev_property")),
          ("se_va"   , (CU.stringToJFloat, "ev_value")),
          // Custom unstructured events
          ("ue_na"   , (decodeString, "ue_name")),
          ("ue_pr"   , (decodeString, "ue_json")),
          ("ue_px"   , (CU.decodeBase64Url, "ue_json")),
          // Ecommerce transactions
          ("tr_id"   , (decodeString, "tr_orderid")),
          ("tr_af"   , (decodeString, "tr_affiliation")),
          ("tr_tt"   , (decodeString, "tr_total")),
          ("tr_tx"   , (decodeString, "tr_tax")),
          ("tr_sh"   , (decodeString, "tr_shipping")),
          ("tr_ci"   , (decodeString, "tr_city")),
          ("tr_st"   , (decodeString, "tr_state")),
          ("tr_co"   , (decodeString, "tr_country")),
          // Ecommerce transaction items
          ("ti_id"   , (decodeString, "ti_orderid")),
          ("ti_sk"   , (decodeString, "ti_sku")),
          ("ti_na"   , (decodeString, "ti_name")),
          ("ti_ca"   , (decodeString, "ti_category")),
          ("ti_pr"   , (decodeString, "ti_price")),
          ("ti_qu"   , (decodeString, "ti_quantity")),
          // Page pings
          ("pp_mix"  , (CU.stringToJInteger, "pp_xoffset_min")),
          ("pp_max"  , (CU.stringToJInteger, "pp_xoffset_max")),
          ("pp_miy"  , (CU.stringToJInteger, "pp_yoffset_min")),
          ("pp_may"  , (CU.stringToJInteger, "pp_yoffset_max")))

    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap

    val transform = event.transform(sourceMap, transformMap)

    // Potentially update the page_url and set the page URL components
    val pageUri = PE.extractPageUri(raw.refererUri, Option(event.page_url))
    pageUri.flatMap(uri => {
      // Update the page_url
      event.page_url = uri.toString

      // Set the URL components
      val components = CU.explodeUri(uri)
      event.page_urlscheme = components.scheme
      event.page_urlhost = components.host
      event.page_urlport = components.port
      event.page_urlpath = components.path.orNull
      event.page_urlquery = components.query.orNull
      event.page_urlfragment = components.fragment.orNull

      uri.success
      })

    // Potentially set the referrer details and URL components
    val refererUri = CU.stringToUri(event.page_referrer)
    refererUri.flatMap(
      uri => {
        uri match {
          case Some(u) =>

            // Set the referrer details
            AE.extractRefererDetails(u, event.page_urlhost) match {
              case Some(refr) =>
                event.refr_medium = refr.medium.toString
                event.refr_source = refr.source.orNull
                event.refr_term = refr.term.orNull
              case _ =>
            }

            // Set the URL components
            val components = CU.explodeUri(u)
            event.refr_urlscheme = components.scheme
            event.refr_urlhost = components.host
            event.refr_urlport = components.port
            event.refr_urlpath = components.path.orNull
            event.refr_urlquery = components.query.orNull
            event.refr_urlfragment = components.fragment.orNull

          case None =>
        }
      uri.success
      })

    // Marketing attribution
    val campaign = pageUri.fold(
      e => unitSuccessNel, // No fields updated
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

    // Some quick and dirty truncation to ensure the load into Redshift doesn't error. Yech this is pretty dirty
    // TODO: move this into the db-specific ETL phase (when written) & _programmatically_ apply to all strings, not just these 6
    event.useragent = CU.truncate(event.useragent, 1000)
    event.page_title = CU.truncate(event.page_title, 2000)
    event.page_referrer = CU.truncate(event.page_referrer, 3000)
    event.page_urlpath = CU.truncate(event.page_urlpath, 1000)
    event.page_urlquery = CU.truncate(event.page_urlquery, 3000)
    event.page_urlfragment = CU.truncate(event.page_urlfragment, 255)

    // Collect our errors on Failure, or return our event on Success
    (useragent.toValidationNel |@| client.toValidationNel |@| pageUri.toValidationNel |@| refererUri.toValidationNel |@| transform |@| campaign) {
      (_,_,_,_,_,_) => event
    }
  }
}
