/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.charset.Charset
import java.net.URI

import cats.Monad
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SelfDescribingData
import io.circe.Json
import org.joda.time.DateTime

import adapters.RawEvent
import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import enrichments.web.{PageEnrichments => WPE}
import utils.{ConversionUtils => CU, JsonUtils => JU}
import utils.MapTransformer._
import utils.shredder.Shredder
import outputs.EnrichedEvent

/**
 * A module to hold our enrichment process.
 * At the moment this is very fixed - no support for configuring enrichments etc.
 */
object EnrichmentManager {

  // Regex for IPv4 without port
  val IPv4Regex = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*""".r

  /**
   * Runs our enrichment process.
   * @param registry Contains configuration for all enrichments to apply
   * @param client Our Iglu client, for schema lookups and validation
   * @param hostEtlVersion ETL version
   * @param etlTstamp ETL timestamp
   * @param raw Our canonical input to enrich
   * @return a MaybeCanonicalOutput - i.e. a ValidationNel containing either failure Strings
   */
  def enrichEvent[F[_]: Monad: RegistryLookup: Clock](
    registry: EnrichmentRegistry,
    client: Client[F, Json],
    hostEtlVersion: String,
    etlTstamp: DateTime,
    raw: RawEvent
  ): F[ValidatedNel[String, EnrichedEvent]] = {
    // 1. Enrichments not expected to fail

    // Let's start populating the CanonicalOutput
    // with the fields which cannot error
    val event = {
      val e = new EnrichedEvent()
      e.event_id = EE.generateEventId // May be updated later if we have an `eid` parameter
      e.v_collector = raw.source.name // May be updated later if we have a `cv` parameter
      e.v_etl = ME.etlVersion(hostEtlVersion)
      e.etl_tstamp = EE.toTimestamp(etlTstamp)
      e.network_userid = raw.context.userId.orNull // May be updated later by 'nuid'
      e.user_ipaddress = ME
        .extractIp("user_ipaddress", raw.context.ipAddress.orNull)
        .toOption
        .orNull // May be updated later by 'ip'
      e
    }

    // 2. Enrichments which can fail

    // 2a. Failable enrichments which don't need the payload

    // Validate that the collectorTstamp exists and is Redshift-compatible
    val collectorTstamp: Either[String, Unit] =
      EE.formatCollectorTstamp(raw.context.timestamp).map { t =>
        event.collector_tstamp = t
        ().asRight
      }

    // Attempt to decode the useragent
    // May be updated later if we have a `ua` parameter
    val useragent: Either[String, Unit] = raw.context.useragent match {
      case Some(ua) =>
        val u = CU.decodeString(Charset.forName(raw.source.encoding), "useragent", ua)
        u.map { ua =>
          event.useragent = ua
          ()
        }
      case None => ().asRight // No fields updated
    }

    // 2b. Failable enrichments using the payload

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    // Caution: by definition, a TransformMap loses type safety. Always unit test!
    val transformMap: TransformMap =
      Map(
        ("e", (EE.extractEventType, "event")),
        ("ip", (ME.extractIp, "user_ipaddress")),
        ("aid", (ME.toTsvSafe, "app_id")),
        ("p", (ME.extractPlatform, "platform")),
        ("tid", (CU.validateInteger, "txn_id")),
        ("uid", (ME.toTsvSafe, "user_id")),
        ("duid", (ME.toTsvSafe, "domain_userid")),
        ("nuid", (ME.toTsvSafe, "network_userid")),
        ("ua", (ME.toTsvSafe, "useragent")),
        ("fp", (ME.toTsvSafe, "user_fingerprint")),
        ("vid", (CU.stringToJInteger, "domain_sessionidx")),
        ("sid", (CU.validateUuid, "domain_sessionid")),
        ("dtm", (EE.extractTimestamp, "dvce_created_tstamp")),
        ("ttm", (EE.extractTimestamp, "true_tstamp")),
        ("stm", (EE.extractTimestamp, "dvce_sent_tstamp")),
        ("tna", (ME.toTsvSafe, "name_tracker")),
        ("tv", (ME.toTsvSafe, "v_tracker")),
        ("cv", (ME.toTsvSafe, "v_collector")),
        ("lang", (ME.toTsvSafe, "br_lang")),
        ("f_pdf", (CU.stringToBooleanlikeJByte, "br_features_pdf")),
        ("f_fla", (CU.stringToBooleanlikeJByte, "br_features_flash")),
        ("f_java", (CU.stringToBooleanlikeJByte, "br_features_java")),
        ("f_dir", (CU.stringToBooleanlikeJByte, "br_features_director")),
        ("f_qt", (CU.stringToBooleanlikeJByte, "br_features_quicktime")),
        ("f_realp", (CU.stringToBooleanlikeJByte, "br_features_realplayer")),
        ("f_wma", (CU.stringToBooleanlikeJByte, "br_features_windowsmedia")),
        ("f_gears", (CU.stringToBooleanlikeJByte, "br_features_gears")),
        ("f_ag", (CU.stringToBooleanlikeJByte, "br_features_silverlight")),
        ("cookie", (CU.stringToBooleanlikeJByte, "br_cookies")),
        ("res", (CE.extractViewDimensions, ("dvce_screenwidth", "dvce_screenheight"))), // Note tuple target
        ("cd", (ME.toTsvSafe, "br_colordepth")),
        ("tz", (ME.toTsvSafe, "os_timezone")),
        ("refr", (ME.toTsvSafe, "page_referrer")),
        ("url", (ME.toTsvSafe, "page_url")), // Note we may override this below
        ("page", (ME.toTsvSafe, "page_title")),
        ("cs", (ME.toTsvSafe, "doc_charset")),
        ("ds", (CE.extractViewDimensions, ("doc_width", "doc_height"))),
        ("vp", (CE.extractViewDimensions, ("br_viewwidth", "br_viewheight"))),
        ("eid", (CU.validateUuid, "event_id")),
        // Custom contexts
        ("co", (JU.extractUnencJson, "contexts")),
        ("cx", (JU.extractBase64EncJson, "contexts")),
        // Custom structured events
        ("ev_ca", (ME.toTsvSafe, "se_category")), // LEGACY tracker var. Leave for backwards compat
        ("ev_ac", (ME.toTsvSafe, "se_action")), // LEGACY tracker var. Leave for backwards compat
        ("ev_la", (ME.toTsvSafe, "se_label")), // LEGACY tracker var. Leave for backwards compat
        ("ev_pr", (ME.toTsvSafe, "se_property")), // LEGACY tracker var. Leave for backwards compat
        ("ev_va", (CU.stringToDoublelike, "se_value")), // LEGACY tracker var. Leave for backwards compat
        ("se_ca", (ME.toTsvSafe, "se_category")),
        ("se_ac", (ME.toTsvSafe, "se_action")),
        ("se_la", (ME.toTsvSafe, "se_label")),
        ("se_pr", (ME.toTsvSafe, "se_property")),
        ("se_va", (CU.stringToDoublelike, "se_value")),
        // Custom unstructured events
        ("ue_pr", (JU.extractUnencJson, "unstruct_event")),
        ("ue_px", (JU.extractBase64EncJson, "unstruct_event")),
        // Ecommerce transactions
        ("tr_id", (ME.toTsvSafe, "tr_orderid")),
        ("tr_af", (ME.toTsvSafe, "tr_affiliation")),
        ("tr_tt", (CU.stringToDoublelike, "tr_total")),
        ("tr_tx", (CU.stringToDoublelike, "tr_tax")),
        ("tr_sh", (CU.stringToDoublelike, "tr_shipping")),
        ("tr_ci", (ME.toTsvSafe, "tr_city")),
        ("tr_st", (ME.toTsvSafe, "tr_state")),
        ("tr_co", (ME.toTsvSafe, "tr_country")),
        // Ecommerce transaction items
        ("ti_id", (ME.toTsvSafe, "ti_orderid")),
        ("ti_sk", (ME.toTsvSafe, "ti_sku")),
        ("ti_na", (ME.toTsvSafe, "ti_name")), // ERROR in Tracker Protocol
        ("ti_nm", (ME.toTsvSafe, "ti_name")),
        ("ti_ca", (ME.toTsvSafe, "ti_category")),
        ("ti_pr", (CU.stringToDoublelike, "ti_price")),
        ("ti_qu", (CU.stringToJInteger, "ti_quantity")),
        // Page pings
        ("pp_mix", (CU.stringToJInteger, "pp_xoffset_min")),
        ("pp_max", (CU.stringToJInteger, "pp_xoffset_max")),
        ("pp_miy", (CU.stringToJInteger, "pp_yoffset_min")),
        ("pp_may", (CU.stringToJInteger, "pp_yoffset_max")),
        // Currency
        ("tr_cu", (ME.toTsvSafe, "tr_currency")),
        ("ti_cu", (ME.toTsvSafe, "ti_currency"))
      )

    val sourceMap: SourceMap = raw.parameters

    val transform: ValidatedNel[String, Int] = event.transform(sourceMap, transformMap)

    // A second TransformMap which can overwrite values set by the first
    val secondPassTransformMap: TransformMap =
      Map(("tnuid", (ME.toTsvSafe, "network_userid"))) // Overwrite collector-set nuid with tracker-set tnuid

    val secondPassTransform: ValidatedNel[String, Int] =
      event.transform(sourceMap, secondPassTransformMap)

    // The load fails if the collector version is not set
    val collectorVersionSet: Either[String, Unit] = event.v_collector match {
      case ("" | null) => "Collector version not set".asLeft
      case _ => ().asRight
    }

    // Potentially update the page_url and set the page URL components
    val pageUri: Either[String, Option[URI]] =
      WPE.extractPageUri(raw.context.refererUri, Option(event.page_url))
    for {
      uri <- pageUri
      u <- uri
    } {
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

    // If our IpToGeo enrichment is enabled, get the geo-location from the IP address
    // enrichment doesn't fail to maintain the previous approach where failures were suppressed
    // c.f. https://github.com/snowplow/snowplow/issues/351
    val geoLocation: Either[String, Unit] = (for {
      enrichment <- registry.getIpLookupsEnrichment
      ip <- Option(event.user_ipaddress)
      result = {
        val ipLookupResult = enrichment.extractIpInformation(ip)
        ipLookupResult.ipLocation.foreach(_.foreach { loc =>
          event.geo_country = loc.countryCode
          event.geo_region = loc.region.orNull
          event.geo_city = loc.city.orNull
          event.geo_zipcode = loc.postalCode.orNull
          event.geo_latitude = loc.latitude
          event.geo_longitude = loc.longitude
          event.geo_region_name = loc.regionName.orNull
          event.geo_timezone = loc.timezone.orNull
        })
        ipLookupResult.isp.foreach(_.foreach { i =>
          event.ip_isp = i
        })
        ipLookupResult.organization.foreach(_.foreach { org =>
          event.ip_organization = org
        })
        ipLookupResult.domain.foreach(_.foreach { d =>
          event.ip_domain = d
        })
        ipLookupResult.connectionType.foreach(_.foreach { ct =>
          event.ip_netspeed = ct
        })
      }
    } yield ().asRight).getOrElse(().asRight)

    // Calculate the derived timestamp
    val derivedTstamp: Either[String, Unit] = EE
      .getDerivedTimestamp(
        Option(event.dvce_sent_tstamp),
        Option(event.dvce_created_tstamp),
        Option(event.collector_tstamp),
        Option(event.true_tstamp)
      )
      .map { dt =>
        dt.foreach(event.derived_tstamp = _)
        ().asRight
      }

    // Fetch IAB enrichment context (before anonymizing the IP address).
    // IAB enrichment is called only if the IP is v4 (and after removing the port if any)
    // and if the user agent is defined.
    val iabContext: Either[String, Option[Json]] = registry.getIabEnrichment match {
      case Some(iab) =>
        event.user_ipaddress match {
          case IPv4Regex(ipv4) if !List(null, "", s"\0").contains(event.useragent) =>
            iab
              .getIabContext(
                Option(event.useragent),
                Option(event.user_ipaddress),
                Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
              )
              .map(_.some)
          case _ => None.asRight
        }
      case None => None.asRight
    }

    // To anonymize the IP address
    Option(event.user_ipaddress).map(ip =>
      event.user_ipaddress = registry.getAnonIpEnrichment match {
        case Some(anon) => anon.anonymizeIp(ip)
        case None => ip
    })

    // Parse the useragent using user-agent-utils
    val uaUtils: Either[String, Unit] = {
      registry.getUserAgentUtilsEnrichment match {
        case Some(uap) => {
          Option(event.useragent) match {
            case Some(ua) =>
              val ca = uap.extractClientAttributes(ua)
              ca.map { c =>
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
                c
              }
              ().asRight
            case None => ().asRight // No fields updated
          }
        }
        case None => ().asRight
      }
    }

    // Create the ua_parser_context
    val uaParser: Either[String, Option[Json]] = {
      registry.getUaParserEnrichment match {
        case Some(uap) =>
          Option(event.useragent) match {
            case Some(ua) => uap.extractUserAgent(ua).map(_.some)
            case None => None.asRight // No fields updated
          }
        case None => None.asRight
      }
    }

    // Finalize the currency conversion
    val currency: Either[NonEmptyList[String], Unit] = {
      registry.getCurrencyConversionEnrichment match {
        case Some(currency) =>
          event.base_currency = currency.baseCurrency.getCode
          // Note that stringToMaybeDouble is applied to either-valid-or-null event POJO
          // properties, so we don't expect any of these four vals to be a Failure
          val trTax = CU.stringToMaybeDouble("tr_tx", event.tr_tax).toValidatedNel
          val tiPrice = CU.stringToMaybeDouble("ti_pr", event.ti_price).toValidatedNel
          val trTotal = CU.stringToMaybeDouble("tr_tt", event.tr_total).toValidatedNel
          val trShipping = CU.stringToMaybeDouble("tr_sh", event.tr_shipping).toValidatedNel
          val convertedCu = ((trTotal, trTax, trShipping, tiPrice)
            .mapN {
              currency.convertCurrencies(
                Option(event.tr_currency),
                _,
                _,
                _,
                Option(event.ti_currency),
                _,
                raw.context.timestamp)
            })
            .toEither
            .flatMap(_.toEither)

          for ((total, tax, shipping, price) <- convertedCu.toOption) {
            event.tr_total_base = total.orNull
            event.tr_tax_base = tax.orNull
            event.tr_shipping_base = shipping.orNull
            event.ti_price_base = price.orNull
          }
          ().asRight
        case None => ().asRight
      }
    }

    // Potentially set the referrer details and URL components
    val refererUri: Either[String, Option[URI]] = CU.stringToUri(event.page_referrer)
    for {
      uri <- refererUri
      u <- uri
    } {

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
        case None => ().asRight
      }
    }

    // Parse the page URI's querystring
    val pageQsMap: Either[String, Option[Map[String, String]]] = pageUri match {
      case Right(Some(u)) =>
        CU.extractQuerystring(u, Charset.forName(raw.source.encoding)).map(_.some)
      case _ => None.asRight
    }

    // Marketing attribution
    val campaign: Either[String, Unit] = pageQsMap match {
      case Right(Some(qsMap)) =>
        registry.getCampaignAttributionEnrichment match {
          case Some(ce) =>
            val cmp = ce.extractMarketingFields(qsMap)
            event.mkt_medium = CU.makeTsvSafe(cmp.medium.orNull)
            event.mkt_source = CU.makeTsvSafe(cmp.source.orNull)
            event.mkt_term = CU.makeTsvSafe(cmp.term.orNull)
            event.mkt_content = CU.makeTsvSafe(cmp.content.orNull)
            event.mkt_campaign = CU.makeTsvSafe(cmp.campaign.orNull)
            event.mkt_clickid = CU.makeTsvSafe(cmp.clickId.orNull)
            event.mkt_network = CU.makeTsvSafe(cmp.network.orNull)
            ().asRight
          case None => ().asRight
        }
      case _ => ().asRight
    }

    // Cross-domain tracking
    val crossDomain: Either[String, Unit] = pageQsMap match {
      case Right(Some(qsMap)) => {
        val crossDomainParseResult = WPE.parseCrossDomain(qsMap)
        for ((maybeRefrDomainUserid, maybeRefrDvceTstamp) <- crossDomainParseResult.toOption) {
          maybeRefrDomainUserid.foreach(event.refr_domain_userid = _: String)
          maybeRefrDvceTstamp.foreach(event.refr_dvce_tstamp = _: String)
        }
        crossDomainParseResult.map(_ => ())
      }
      case _ => ().asRight
    }

    // This enrichment cannot fail
    (registry.getEventFingerprintEnrichment match {
      case Some(efe) => event.event_fingerprint = efe.getEventFingerprint(sourceMap)
      case _ => ()
    })

    // Validate custom contexts
    val customContexts: F[ValidatedNel[String, List[SelfDescribingData[Json]]]] =
      Shredder.extractAndValidateCustomContexts(event, client)

    // Validate unstructured event
    val unstructEvent: F[ValidatedNel[String, List[SelfDescribingData[Json]]]] =
      Shredder.extractAndValidateUnstructEvent(event, client)

    // Extract the event vendor/name/format/version
    val extractSchema: F[Either[String, Unit]] = SchemaEnrichment
      .extractSchema(event, client)
      .map { _.map { schemaKey =>
        event.event_vendor = schemaKey.vendor
        event.event_name = schemaKey.name
        event.event_format = schemaKey.format
        event.event_version = schemaKey.version.asString
        ().asRight
      } }

    // Execute the JavaScript scripting enrichment
    val jsScript: Either[String, List[Json]] = registry.getJavascriptScriptEnrichment match {
      case Some(jse) => jse.process(event)
      case None => Nil.asRight
    }

    // Execute cookie extractor enrichment
    val cookieExtractorContext: List[Json] = registry.getCookieExtractorEnrichment match {
      case Some(cee) => cee.extract(raw.context.headers)
      case None => Nil
    }

    // Execute header extractor enrichment
    val httpHeaderExtractorContext: List[Json] = registry.getHttpHeaderExtractorEnrichment match {
      case Some(hee) => hee.extract(raw.context.headers)
      case None => Nil
    }

    // Fetch weather context
    val weatherContext: Either[String, Option[Json]] = registry.getWeatherEnrichment match {
      case Some(we) =>
        we.getWeatherContext(
            Option(event.geo_latitude),
            Option(event.geo_longitude),
            Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
          )
          .map(_.some)
      case None => None.asRight
    }

    // YAUAA enrichment
    val yauaaContext: Either[String, Option[Json]] = registry.getYauaaEnrichment match {
      case Some(yauaaEnrichment) =>
        yauaaEnrichment
          .getYauaaContext(event.useragent)
          .map(_.some)
      case None => None.success
    }

    // Assemble array of contexts prepared by built-in enrichments
    val preparedDerivedContexts: List[Json] = List(uaParser).collect {
      case Right(Some(context)) => context
    } ++ List(weatherContext).collect {
      case Success(Some(context)) => context
    } ++ List(yauaaContext).collect {
      case Success(Some(context)) => context
    } ++ List(iabContext).collect {
      case Right(Some(context)) => context
    } ++ jsScript.getOrElse(Nil) ++ cookieExtractorContext ++ httpHeaderExtractorContext

    // Derive some contexts with custom SQL Query enrichment
    val sqlQueryContexts: F[ValidatedNel[String, List[Json]]] =
      registry.getSqlQueryEnrichment match {
        case Some(enrichment) =>
          customContexts.product(unstructEvent).map(_ match {
            case (Validated.Valid(cctx), Validated.Valid(ue)) =>
              enrichment.lookup(event, preparedDerivedContexts, cctx, ue)
            case _ =>
              // Skip. Unstruct event or custom context corrupted (event enrichment will fail)
              Nil.validNel
          })
        case None => Monad[F].pure(Nil.validNel)
      }

    // Derive some contexts with custom API Request enrichment
    val apiRequestContexts: F[ValidatedNel[String, List[Json]]] =
      registry.getApiRequestEnrichment match {
        case Some(enrichment) =>
          customContexts.product(unstructEvent).product(sqlQueryContexts).map(_ match {
            case ((Validated.Valid(cctx), Validated.Valid(ue)), Validated.Valid(sctx)) =>
              enrichment.lookup(event, preparedDerivedContexts ++ sctx, cctx, ue)
            case _ =>
              // Skip. Unstruct event or custom context corrupted, event enrichment will fail anyway
              Nil.validNel
          })
        case None => Monad[F].pure(Nil.validNel)
      }

    // Assemble prepared derived contexts with fetched via API Request
    val derivedContexts: F[List[Json]] = for {
      api <- apiRequestContexts
      sql <- sqlQueryContexts
    } yield api.getOrElse(Nil) ++ sql.getOrElse(Nil) ++ preparedDerivedContexts

    val formatDerivedContexts: F[Unit] = derivedContexts.map { d =>
      if (d.nonEmpty) {
        event.derived_contexts = ME.formatDerivedContexts(d)
      } else {
        ()
      }
    }

    val piiTransform = registry.getPiiPseudonymizerEnrichment match {
      case Some(enrichment) => enrichment.transformer(event)
      case None => Nil
    }

    // Collect our errors on Failure, or return our event on Success
    (
      customContexts,
      unstructEvent,
      apiRequestContexts,
      sqlQueryContexts,
      extractSchema,
      formatDerivedContexts
    ).mapN { (cc, ue, api, sql, es, _) =>
      val first = (
        useragent.toValidatedNel,
        collectorTstamp.toValidatedNel,
        derivedTstamp.toValidatedNel,
        uaUtils.toValidatedNel,
        uaParser.toValidatedNel,
        collectorVersionSet.toValidatedNel,
        pageUri.toValidatedNel,
        crossDomain.toValidatedNel,
        geoLocation.toValidatedNel,
        refererUri.toValidatedNel,
        transform,
        currency.toValidated,
        secondPassTransform,
        pageQsMap.toValidatedNel,
        jsScript.toValidatedNel,
        campaign.toValidatedNel,
        cc,
        ue,
        api,
        sql,
        es.toValidatedNel,
        weatherContext.toValidatedNel
      ).mapN((_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => ())

      val second =(
        yauaaContext.toValidatedNel,
        iabContext.toValidatedNel,
        piiTransform.valid
      ).mapN((_, _) => ())

      (first, second).mapN((_, _) => event)
    }
  }
}
