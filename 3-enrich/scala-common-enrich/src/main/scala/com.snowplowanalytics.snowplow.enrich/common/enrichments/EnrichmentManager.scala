/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments

// Scala
import scala.collection.mutable.ListBuffer

// Scalaz
import scalaz._
import Scalaz._

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpGeo

// This project
import inputs.{CanonicalInput, NvGetPayload}
import outputs.CanonicalOutput

import utils.{ConversionUtils => CU}
import utils.{JsonUtils => JU}
import utils.MapTransformer._

import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import enrichments.{GeoEnrichments => GE}
import enrichments.{PrivacyEnrichments => PE}
import PE.AnonOctets.AnonOctets
import web.{PageEnrichments => WPE}
import web.{AttributionEnrichments => WAE}

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
  def enrichEvent(geo: IpGeo, hostEtlVersion: String, anonOctets: AnonOctets, raw: CanonicalInput): ValidatedCanonicalOutput = {

    // Placeholders for where the Success value doesn't matter.
    // Useful when you're updating large (>22 field) POSOs.
    val unitSuccess = ().success[String]
    val unitSuccessNel = ().successNel[String]

    // Retrieve the payload
    // TODO: add support for other
    // payload types in the future
    val parameters = raw.payload match {
      case NvGetPayload(p) => p
      case _ => throw new FatalEtlError("Only name-value pair GET payloads are currently supported") // TODO: change back to FatalEtlException when Cascading FailureTrap supports exclusions
    }

    // 1. Enrichments not expected to fail

    // Let's start populating the CanonicalOutput
    // with the fields which cannot error
    val event = new CanonicalOutput().tap { e =>
      e.collector_tstamp = EE.toTimestamp(raw.timestamp)
      e.event_id = EE.generateEventId
      e.v_collector = raw.source.collector // May be updated later if we have a `cv` parameter
      e.v_etl = ME.etlVersion(hostEtlVersion)
      raw.ipAddress.map(ip => e.user_ipaddress = PE.anonymizeIp(ip, anonOctets))
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
          event.dvce_ismobile = CU.booleanToJByte(c.deviceIsMobile)
          c.success
          })
        ca
      case None => unitSuccess // No fields updated
    }

    // 2b. Failable enrichments using the payload

    // Partially apply functions which need an encoding, to create a TransformFunc
    val MaxJsonLength = 10000
    val extractUrlEncJson: TransformFunc = JU.extractUrlEncJson(MaxJsonLength, raw.encoding, _, _)
    val extractBase64EncJson: TransformFunc = JU.extractBase64EncJson(MaxJsonLength, _, _)

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    // Caution: by definition, a TransformMap loses type safety. Always unit test!
    val transformMap: TransformMap =
      Map(("e"       , (EE.extractEventType, "event")),
          ("evn"     , (ME.toTsvSafe, "event_vendor")),
          ("ip"      , (ME.toTsvSafe, "user_ipaddress")),
          ("aid"     , (ME.toTsvSafe, "app_id")),
          ("p"       , (ME.extractPlatform, "platform")),
          ("tid"     , (ME.toTsvSafe, "txn_id")),
          ("uid"     , (ME.toTsvSafe, "user_id")),
          ("duid"    , (ME.toTsvSafe, "domain_userid")),
          ("nuid"    , (ME.toTsvSafe, "network_userid")),
          ("fp"      , (ME.toTsvSafe, "user_fingerprint")),
          ("vid"     , (CU.stringToJInteger, "domain_sessionidx")),
          ("dtm"     , (EE.extractTimestamp, "dvce_tstamp")),
          ("tna"     , (ME.toTsvSafe, "name_tracker")),
          ("tv"      , (ME.toTsvSafe, "v_tracker")),
          ("cv"      , (ME.toTsvSafe, "v_collector")),
          ("lang"    , (ME.toTsvSafe, "br_lang")),
          ("f_pdf"   , (CU.stringToBooleanlikeJByte, "br_features_pdf")),
          ("f_fla"   , (CU.stringToBooleanlikeJByte, "br_features_flash")),
          ("f_java"  , (CU.stringToBooleanlikeJByte, "br_features_java")),
          ("f_dir"   , (CU.stringToBooleanlikeJByte, "br_features_director")),
          ("f_qt"    , (CU.stringToBooleanlikeJByte, "br_features_quicktime")),
          ("f_realp" , (CU.stringToBooleanlikeJByte, "br_features_realplayer")),
          ("f_wma"   , (CU.stringToBooleanlikeJByte, "br_features_windowsmedia")),
          ("f_gears" , (CU.stringToBooleanlikeJByte, "br_features_gears")),
          ("f_ag"    , (CU.stringToBooleanlikeJByte, "br_features_silverlight")),
          ("cookie"  , (CU.stringToBooleanlikeJByte, "br_cookies")),
          ("res"     , (CE.extractViewDimensions, ("dvce_screenwidth", "dvce_screenheight"))), // Note tuple target
          ("cd"      , (ME.toTsvSafe, "br_colordepth")),
          ("tz"      , (ME.toTsvSafe, "os_timezone")),
          ("refr"    , (ME.toTsvSafe, "page_referrer")),
          ("url"     , (ME.toTsvSafe, "page_url")), // Note we may override this below
          ("page"    , (ME.toTsvSafe, "page_title")),
          ("cs"      , (ME.toTsvSafe, "doc_charset")),
          ("ds"      , (CE.extractViewDimensions, ("doc_width", "doc_height"))),
          ("vp"      , (CE.extractViewDimensions, ("br_viewwidth", "br_viewheight"))),
          // Custom contexts
          ("co"   , (extractUrlEncJson, "contexts")),
          ("cx"   , (extractBase64EncJson, "contexts")),
          // Custom structured events
          ("ev_ca"   , (ME.toTsvSafe, "se_category")),   // LEGACY tracker var. Leave for backwards compat
          ("ev_ac"   , (ME.toTsvSafe, "se_action")),     // LEGACY tracker var. Leave for backwards compat
          ("ev_la"   , (ME.toTsvSafe, "se_label")),      // LEGACY tracker var. Leave for backwards compat
          ("ev_pr"   , (ME.toTsvSafe, "se_property")),   // LEGACY tracker var. Leave for backwards compat
          ("ev_va"   , (CU.stringToDoublelike, "se_value")), // LEGACY tracker var. Leave for backwards compat
          ("se_ca"   , (ME.toTsvSafe, "se_category")),
          ("se_ac"   , (ME.toTsvSafe, "se_action")),
          ("se_la"   , (ME.toTsvSafe, "se_label")),
          ("se_pr"   , (ME.toTsvSafe, "se_property")),
          ("se_va"   , (CU.stringToDoublelike, "se_value")),
          // Custom unstructured events
          ("ue_na"   , (ME.toTsvSafe, "ue_name")),
          ("ue_pr"   , (extractUrlEncJson, "ue_properties")),
          ("ue_px"   , (extractBase64EncJson, "ue_properties")),
          // Ecommerce transactions
          ("tr_id"   , (ME.toTsvSafe, "tr_orderid")),
          ("tr_af"   , (ME.toTsvSafe, "tr_affiliation")),
          ("tr_tt"   , (CU.stringToDoublelike, "tr_total")),
          ("tr_tx"   , (CU.stringToDoublelike, "tr_tax")),
          ("tr_sh"   , (CU.stringToDoublelike, "tr_shipping")),
          ("tr_ci"   , (ME.toTsvSafe, "tr_city")),
          ("tr_st"   , (ME.toTsvSafe, "tr_state")),
          ("tr_co"   , (ME.toTsvSafe, "tr_country")),
          // Ecommerce transaction items
          ("ti_id"   , (ME.toTsvSafe, "ti_orderid")),
          ("ti_sk"   , (ME.toTsvSafe, "ti_sku")),
          ("ti_na"   , (ME.toTsvSafe, "ti_name")),
          ("ti_ca"   , (ME.toTsvSafe, "ti_category")),
          ("ti_pr"   , (CU.stringToDoublelike, "ti_price")),
          ("ti_qu"   , (ME.toTsvSafe, "ti_quantity")),
          // Page pings
          ("pp_mix"  , (CU.stringToJInteger, "pp_xoffset_min")),
          ("pp_max"  , (CU.stringToJInteger, "pp_xoffset_max")),
          ("pp_miy"  , (CU.stringToJInteger, "pp_yoffset_min")),
          ("pp_may"  , (CU.stringToJInteger, "pp_yoffset_max")))

    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap
  
    val transform = event.transform(sourceMap, transformMap)

    // Potentially update the page_url and set the page URL components
    val pageUri = WPE.extractPageUri(raw.refererUri, Option(event.page_url))
    for (uri <- pageUri; u <- uri) {
      // Update the page_url
      event.page_url = u.toString

      // Set the URL components
      val components = CU.explodeUri(u)
      event.page_urlscheme = components.scheme
      event.page_urlhost = components.host
      event.page_urlport = components.port
      event.page_urlpath = components.path.orNull
      event.page_urlquery = components.query.orNull
      event.page_urlfragment = components.fragment.orNull
    }

    // Get the geo-location from the IP address
    val geoLocation = GE.extractGeoLocation(geo, raw.ipAddress.orNull)
    for (loc <- geoLocation; l <- loc) {
      event.geo_country = l.countryCode
      event.geo_region = l.region.orNull
      event.geo_city = l.city.orNull
      event.geo_zipcode = l.postalCode.orNull
      event.geo_latitude = l.latitude
      event.geo_longitude = l.longitude
    }

    // Potentially set the referrer details and URL components
    val refererUri = CU.stringToUri(event.page_referrer)
    for (uri <- refererUri; u <- uri) {
      
      // Set the URL components
      val components = CU.explodeUri(u)
      event.refr_urlscheme = components.scheme
      event.refr_urlhost = components.host
      event.refr_urlport = components.port
      event.refr_urlpath = components.path.orNull
      event.refr_urlquery = components.query.orNull
      event.refr_urlfragment = components.fragment.orNull

      // Set the referrer details
      for (refr <- WAE.extractRefererDetails(u, event.page_urlhost)) {
        event.refr_medium = refr.medium.toString
        event.refr_source = refr.source.orNull
        event.refr_term = refr.term.orNull
      }
    }

    // Marketing attribution
    val campaign = pageUri.fold(
      e => unitSuccessNel, // No fields updated
      uri => uri match {
        case Some(u) =>
          WAE.extractMarketingFields(u, raw.encoding).flatMap(cmp => {
            event.mkt_medium = cmp.medium
            event.mkt_source = cmp.source
            event.mkt_term = cmp.term
            event.mkt_content = cmp.content
            event.mkt_campaign = cmp.campaign
            cmp.success
            })
        case None => unitSuccessNel // No fields updated
        })

    // Some quick and dirty truncation to ensure the load into Redshift doesn't error. Yech this is pretty dirty
    // TODO: move this into the db-specific ETL phase (when written) & _programmatically_ apply to all strings, not just these 6
    event.useragent = CU.truncate(event.useragent, 1000)
    event.page_title = CU.truncate(event.page_title, 2000)
    event.page_urlpath = CU.truncate(event.page_urlpath, 1000)
    event.page_urlquery = CU.truncate(event.page_urlquery, 3000)
    event.page_urlfragment = CU.truncate(event.page_urlfragment, 255)
    event.refr_urlpath = CU.truncate(event.refr_urlpath, 1000)
    event.refr_urlquery = CU.truncate(event.refr_urlquery, 3000)
    event.refr_urlfragment = CU.truncate(event.refr_urlfragment, 255)
    event.refr_term = CU.truncate(event.refr_term, 255)
    event.se_label = CU.truncate(event.se_label, 255)

    // Collect our errors on Failure, or return our event on Success 
    (useragent.toValidationNel |@| client.toValidationNel |@| pageUri.toValidationNel |@| geoLocation.toValidationNel |@| refererUri.toValidationNel |@| transform |@| campaign) {
      (_,_,_,_,_,_,_) => event
    }
  }
}
