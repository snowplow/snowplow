/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments

// Joda
import org.joda.time.DateTime

// Iglu
import iglu.client.Resolver

// Scalaz
import scalaz._
import Scalaz._

// SnowPlow Utils
import util.Tap._

// This project
import adapters.RawEvent
import outputs.EnrichedEvent

import utils.{ConversionUtils => CU, JsonUtils => JU}
import utils.MapTransformer._
import utils.shredder.Shredder

import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import web.{PageEnrichments => WPE}

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
   * @param registry Contains configuration
   *        for all enrichments to apply
   * @param hostEtlVersion ETL version
   * @param etlTstamp ETL timestamp
   * @param raw Our canonical input
   *        to enrich
   * @return a MaybeCanonicalOutput - i.e.
   *         a ValidationNel containing
   *         either failure Strings or a
   *         NonHiveOutput.
   */
  def enrichEvent(registry: EnrichmentRegistry, hostEtlVersion: String, etlTstamp: DateTime, raw: RawEvent)(implicit resolver: Resolver): ValidatedEnrichedEvent = {

    // Placeholders for where the Success value doesn't matter.
    // Useful when you're updating large (>22 field) POSOs.
    val unitSuccess = ().success[String]
    val unitSuccessNel = ().successNel[String]

    // 1. Enrichments not expected to fail

    // Let's start populating the CanonicalOutput
    // with the fields which cannot error
    val event = new EnrichedEvent().tap { e =>
      e.event_id = EE.generateEventId      // May be updated later if we have an `eid` parameter
      e.v_collector = raw.source.name // May be updated later if we have a `cv` parameter
      e.v_etl = ME.etlVersion(hostEtlVersion)
      e.etl_tstamp = EE.toTimestamp(etlTstamp)
      e.network_userid = raw.context.userId.orNull    // May be updated later by 'nuid'
      e.user_ipaddress = raw.context.ipAddress.orNull // May be updated later by 'ip'
    }

    // 2. Enrichments which can fail

    // 2a. Failable enrichments which don't need the payload

    // Validate that the collectorTstamp exists and is Redshift-compatible
    val collectorTstamp = EE.formatCollectorTstamp(raw.context.timestamp) match {
      case Success(t) => {
        event.collector_tstamp = t
        unitSuccess
      }
      case f => f
    }

    // Attempt to decode the useragent
    // May be updated later if we have a `ua` parameter
    val useragent = raw.context.useragent match {
      case Some(ua) =>
        val u = CU.decodeString(raw.source.encoding, "useragent", ua)
        u.flatMap(ua => {
          event.useragent = ua
          ua.success
          })
      case None => unitSuccess // No fields updated
    }

    // 2b. Failable enrichments using the payload

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    // Caution: by definition, a TransformMap loses type safety. Always unit test!
    val transformMap: TransformMap =
      Map(("e"       , (EE.extractEventType, "event")),
          ("ip"      , (ME.toTsvSafe, "user_ipaddress")),
          ("aid"     , (ME.toTsvSafe, "app_id")),
          ("p"       , (ME.extractPlatform, "platform")),
          ("tid"     , (CU.validateInteger, "txn_id")),
          ("uid"     , (ME.toTsvSafe, "user_id")),
          ("duid"    , (ME.toTsvSafe, "domain_userid")),
          ("nuid"    , (ME.toTsvSafe, "network_userid")),
          ("ua"      , (ME.toTsvSafe, "useragent")),
          ("fp"      , (ME.toTsvSafe, "user_fingerprint")),
          ("vid"     , (CU.stringToJInteger, "domain_sessionidx")),
          ("sid"     , (CU.validateUuid, "domain_sessionid")),
          ("dtm"     , (EE.extractTimestamp, "dvce_created_tstamp")),
          ("ttm"     , (EE.extractTimestamp, "true_tstamp")),
          ("stm"     , (EE.extractTimestamp, "dvce_sent_tstamp")),
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
          ("eid"     , (CU.validateUuid, "event_id")),
          // Custom contexts
          ("co"   , (JU.extractUnencJson, "contexts")),
          ("cx"   , (JU.extractBase64EncJson, "contexts")),
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
          ("ue_pr"   , (JU.extractUnencJson, "unstruct_event")),
          ("ue_px"   , (JU.extractBase64EncJson, "unstruct_event")),
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
          ("ti_na"   , (ME.toTsvSafe, "ti_name")),       // ERROR in Tracker Protocol
          ("ti_nm"   , (ME.toTsvSafe, "ti_name")),
          ("ti_ca"   , (ME.toTsvSafe, "ti_category")),
          ("ti_pr"   , (CU.stringToDoublelike, "ti_price")),
          ("ti_qu"   , (ME.toTsvSafe, "ti_quantity")),
          // Page pings
          ("pp_mix"  , (CU.stringToJInteger, "pp_xoffset_min")),
          ("pp_max"  , (CU.stringToJInteger, "pp_xoffset_max")),
          ("pp_miy"  , (CU.stringToJInteger, "pp_yoffset_min")),
          ("pp_may"  , (CU.stringToJInteger, "pp_yoffset_max")),
          // Currency
          ("tr_cu"  , (ME.toTsvSafe, "tr_currency")),
          ("ti_cu"  , (ME.toTsvSafe, "ti_currency")))

    val sourceMap: SourceMap = raw.parameters

    val transform = event.transform(sourceMap, transformMap)

    // A second TransformMap which can overwrite values set by the first
    val secondPassTransformMap: TransformMap =
      Map(("tnuid"   , (ME.toTsvSafe, "network_userid"))) // Overwrite collector-set nuid with tracker-set tnuid

    val secondPassTransform = event.transform(sourceMap, secondPassTransformMap)

    // The load fails if the collector version is not set
    val collectorVersionSet = event.v_collector match {
      case ("" | null) => "Collector version not set".fail
      case _ => unitSuccess
    }

    // Potentially update the page_url and set the page URL components
    val pageUri = WPE.extractPageUri(raw.context.refererUri, Option(event.page_url))
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

    // If our IpToGeo enrichment is enabled,
    // get the geo-location from the IP address
    val geoLocation = {
      registry.getIpLookupsEnrichment match {
        case Some(geo) => {
          Option(event.user_ipaddress) match {
            case Some(address) => {
              val ipLookupResult = geo.extractIpInformation(address)
              for (res <- ipLookupResult) {
                for ( loc <- res._1) {
                  event.geo_country = loc.countryCode
                  event.geo_region = loc.region.orNull
                  event.geo_city = loc.city.orNull
                  event.geo_zipcode = loc.postalCode.orNull
                  event.geo_latitude = loc.latitude
                  event.geo_longitude = loc.longitude
                  event.geo_region_name = loc.regionName.orNull
                  event.geo_timezone = loc.timezone.orNull
                }
                event.ip_isp = res._2.orNull
                event.ip_organization = res._3.orNull
                event.ip_domain = res._4.orNull
                event.ip_netspeed = res._5.orNull
              }
              ipLookupResult
            }
            case None => unitSuccess
          }
        }
        case None => unitSuccess
      }
    }

    // To anonymize the IP address
    Option(event.user_ipaddress).map(ip => event.user_ipaddress = registry.getAnonIpEnrichment match {
      case Some(anon) => anon.anonymizeIp(ip)
      case None => ip
    })

    // Parse the useragent using user-agent-utils
    val client = {
      registry.getUserAgentUtilsEnrichment match {
        case Some(uap) => {
          Option(event.useragent) match {
            case Some(ua) =>
              val ca = uap.extractClientAttributes(ua)
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
        }
        case None => unitSuccess
      }
    }

    // Create the ua_parser_context
    val uaParser = {
      registry.getUaParserEnrichment match {
        case Some(uap) => {
          Option(event.useragent) match {
            case Some(ua) => uap.extractUserAgent(ua).map(_.some)
            case None => None.success // No fields updated
          }
        }
        case None => None.success
      }
    }

    // Finalize the currency conversion
    val currency = {
      registry.getCurrencyConversionEnrichment match {
        case Some(currency) => {
          event.base_currency = currency.baseCurrency
          // Note that stringToMaybeDouble is applied to either-valid-or-null event POJO
          // properties, so we don't expect any of these four vals to be a Failure
          val trTax      = CU.stringToMaybeDouble("tr_tx", event.tr_tax).toValidationNel
          val tiPrice    = CU.stringToMaybeDouble("ti_pr", event.ti_price).toValidationNel
          val trTotal    = CU.stringToMaybeDouble("tr_tt", event.tr_total).toValidationNel
          val trShipping = CU.stringToMaybeDouble("tr_sh", event.tr_shipping).toValidationNel
          val convertedCu = ((trTotal |@| trTax |@| trShipping |@| tiPrice) {
            currency.convertCurrencies(Option(event.tr_currency), _, _, _, Option(event.ti_currency), _, raw.context.timestamp)
          }).flatMap(x => x)

          for ((total, tax, shipping, price) <- convertedCu.toOption) {
            event.tr_total_base = total.orNull
            event.tr_tax_base = tax.orNull
            event.tr_shipping_base = shipping.orNull
            event.ti_price_base = price.orNull
          }

          convertedCu
        }
        case None => unitSuccess.toValidationNel
      }
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
      registry.getRefererParserEnrichment match {
        case Some(rp) => {
          for (refr <- rp.extractRefererDetails(u, event.page_urlhost)) {
            event.refr_medium = CU.makeTsvSafe(refr.medium.toString)
            event.refr_source = CU.makeTsvSafe(refr.source.orNull)
            event.refr_term = CU.makeTsvSafe(refr.term.orNull)
          }
        }
        case None => unitSuccess
      }
    }

    // Parse the page URI's querystring
    val pageQsMap = pageUri match {
      case Success(Some(u)) => CU.extractQuerystring(u, raw.source.encoding).map(_.some)
      case _ => Success(None)
    }

    // Marketing attribution
    val campaign = pageQsMap match {
      case Success(Some(qsMap)) => registry.getCampaignAttributionEnrichment match {
        case Some(ce) =>
          ce.extractMarketingFields(qsMap).flatMap(cmp => {
            event.mkt_medium = CU.makeTsvSafe(cmp.medium.orNull)
            event.mkt_source = CU.makeTsvSafe(cmp.source.orNull)
            event.mkt_term = CU.makeTsvSafe(cmp.term.orNull)
            event.mkt_content = CU.makeTsvSafe(cmp.content.orNull)
            event.mkt_campaign = CU.makeTsvSafe(cmp.campaign.orNull)
            event.mkt_clickid = CU.makeTsvSafe(cmp.clickId.orNull)
            event.mkt_network = CU.makeTsvSafe(cmp.network.orNull)
            cmp.success
          })
        case None => unitSuccessNel
      }
      case _ => unitSuccessNel
    }

    // Cross-domain tracking
    val crossDomain = pageQsMap match {
      case Success(Some(qsMap)) => {
        val crossDomainParseResult = WPE.parseCrossDomain(qsMap)
        for ((maybeRefrDomainUserid, maybeRefrDvceTstamp) <- crossDomainParseResult.toOption) {
          maybeRefrDomainUserid.foreach(event.refr_domain_userid = _: String)
          maybeRefrDvceTstamp.foreach(event.refr_dvce_tstamp = _: String)
        }
        crossDomainParseResult
      }
      case _ => unitSuccess
    }

    // This enrichment cannot fail
    (registry.getEventFingerprintEnrichment match {
      case Some(efe) => event.event_fingerprint = efe.getEventFingerprint(sourceMap)
      case _ => ()
    })

    // Validate contexts and unstructured events
    val shred = Shredder.shred(event) match {
      case Failure(msgs) => msgs.map(_.toString).fail
      case Success(_) => unitSuccess.toValidationNel
    }

    // Extract the event vendor/name/format/version
    val extractSchema = SchemaEnrichment.extractSchema(event).map(schemaKey => {
      event.event_vendor = schemaKey.vendor
      event.event_name = schemaKey.name
      event.event_format = schemaKey.format
      event.event_version = schemaKey.version
      unitSuccess
    })

    // Calculate the derived timestamp
    val derivedTstamp = EE.getDerivedTimestamp(
      Option(event.dvce_sent_tstamp),
      Option(event.dvce_created_tstamp),
      Option(event.collector_tstamp),
      Option(event.true_tstamp)
    ) match {
      case Success(dt) => {
        dt.foreach(event.derived_tstamp = _)
        unitSuccess
      }
      case f => f
    }

    // Execute the JavaScript scripting enrichment
    val jsScript = registry.getJavascriptScriptEnrichment match {
      case Some(jse) => jse.process(event)
      case None => Nil.success
    }

    // Execute cookie extractor enrichment
    val cookieExtractorContext = registry.getCookieExtractorEnrichment match {
      case Some(cee) =>
        val headers = raw.context.headers

        cee.extract(headers)
      case None => Nil
    }

    // Fetch weather context
    val weatherContext = registry.getWeatherEnrichment match {
      case Some(we) => {
        we.getWeatherContext(
          Option(event.geo_latitude),
          Option(event.geo_longitude),
          Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)).map(_.some)
      }
      case None => None.success
    }

    // Assemble array of derived contexts
    val derived_contexts = List(uaParser).collect {
      case Success(Some(context)) => context
    } ++ List(weatherContext).collect {
     case Success(Some(context)) => context
    } ++ jsScript.getOrElse(Nil) ++ cookieExtractorContext

    if (derived_contexts.size > 0) {
      event.derived_contexts = ME.formatDerivedContexts(derived_contexts)
    }

    // Collect our errors on Failure, or return our event on Success
    // Broken into two parts due to 12 argument limit on |@|
    val first =
      (useragent.toValidationNel              |@|
      collectorTstamp.toValidationNel         |@|
      derivedTstamp.toValidationNel           |@|
      client.toValidationNel                  |@|
      uaParser.toValidationNel                |@|
      collectorVersionSet.toValidationNel     |@|
      pageUri.toValidationNel                 |@|
      geoLocation.toValidationNel             |@|
      refererUri.toValidationNel) {
      (_,_,_,_,_,_,_,_,_) => ()
    }
    val second = 
      (transform                              |@|
      currency                                |@|
      secondPassTransform                     |@|
      pageQsMap.toValidationNel               |@|
      crossDomain.toValidationNel             |@|
      jsScript.toValidationNel                |@|
      campaign                                |@|
      shred                                   |@|
      extractSchema.toValidationNel           |@|
      weatherContext.toValidationNel) {
      (_,_,_,_,_,_,_,_,_,_) => ()
    }
    (first |@| second) {
      (_,_) => event
    }
  }
}
