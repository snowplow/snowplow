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
import cats.data.{EitherT, NonEmptyList, OptionT, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.EnrichmentFailureMessage._
import com.snowplowanalytics.snowplow.badrows.Payload._
import com.snowplowanalytics.refererparser._
import io.circe.Json
import org.joda.time.DateTime

import adapters.RawEvent
import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.{ClientEnrichments => CE}
import enrichments.registry._
import enrichments.registry.apirequest.ApiRequestEnrichment
import enrichments.registry.pii.PiiPseudonymizerEnrichment
import enrichments.registry.sqlquery.SqlQueryEnrichment
import enrichments.web.{PageEnrichments => WPE}
import outputs.EnrichedEvent
import utils.{ConversionUtils => CU, JsonUtils => JU}
import utils.MapTransformer._
import utils.shredder.Shredder

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
    registry: EnrichmentRegistry[F],
    client: Client[F, Json],
    processor: Processor,
    etlTstamp: DateTime,
    raw: RawEvent
  ): F[Validated[(NonEmptyList[EnrichmentStageIssue], PartiallyEnrichedEvent), EnrichedEvent]] = {
    // 1. Enrichments not expected to fail

    // Let's start populating the CanonicalOutput
    // with the fields which cannot error
    val event = setupEnrichedEvent(raw, etlTstamp, processor)

    // 2. Enrichments which can fail

    // 2a. Failable enrichments which don't need the payload

    // Validate that the collectorTstamp exists and is Redshift-compatible
    val collectorTstamp: Either[EnrichmentStageIssue, Unit] =
      setCollectorTstamp(event, raw.context.timestamp)

    // Attempt to decode the useragent
    // May be updated later if we have a `ua` parameter
    val useragent: Either[EnrichmentStageIssue, Unit] =
      setUseragent(event, raw.context.useragent, raw.source.encoding)

    // 2b. Failable enrichments using the payload
    val sourceMap: SourceMap = raw.parameters
    val firstPassTransform = event.transform(sourceMap, firstPassTransformMap)
    val secondPassTransform = event.transform(sourceMap, secondPassTransformMap)

    // The load fails if the collector version is not set
    val collectorVersionSet: Either[EnrichmentStageIssue, Unit] = getCollectorVersionSet(event)

    // Potentially update the page_url and set the page URL components
    val pageUri: Either[EnrichmentStageIssue, Option[URI]] =
      getPageUri(raw.context.refererUri, event)

    // Calculate the derived timestamp
    val derivedTstamp: Either[EnrichmentStageIssue, Unit] = getDerivedTstamp(event)

    // Fetch IAB enrichment context (before anonymizing the IP address)
    val iabContext: Either[NonEmptyList[EnrichmentStageIssue], Option[Json]] =
      getIabContext(event, registry.iab)

    // To anonymize the IP address
    anonIp(event, registry.anonIp).foreach(event.user_ipaddress = _)

    // Parse the useragent using user-agent-utils
    val uaUtils: Either[EnrichmentStageIssue, Unit] = getUaUtils(event, registry.userAgentUtils)

    // Create the ua_parser_context
    val uaParser: Either[EnrichmentStageIssue, Option[Json]] = getUaParser(event, registry.uaParser)

    // Finalize the currency conversion
    val currency: F[Either[NonEmptyList[EnrichmentStageIssue], Unit]] =
      getCurrency(event, raw.context.timestamp, registry.currencyConversion)

    // Potentially set the referrer details and URL components
    val refererUri: Either[EnrichmentStageIssue, Option[URI]] =
      getRefererUri(event, registry.refererParser)

    // Parse the page URI's querystring
    val pageQsMap: Either[EnrichmentStageIssue, Option[Map[String, String]]] =
      extractQueryString(pageUri, raw.source.encoding)

    // Marketing attribution
    getCampaign(event, pageQsMap, registry.campaignAttribution)

    // Cross-domain tracking
    val crossDomain: Either[EnrichmentStageIssue, Unit] = getCrossDomain(event, pageQsMap)

    // This enrichment cannot fail
    setEventFingerprint(event, sourceMap, registry.eventFingerprint)

    // Extract the event vendor/name/format/version
    val extractSchema: F[Either[EnrichmentStageIssue, Unit]] = extractSchemaFields(event, client)

    // Execute the JavaScript scripting enrichment
    val jsScript: Either[EnrichmentStageIssue, List[Json]] =
      getJsScript(event, registry.javascriptScript)

    // Execute cookie extractor enrichment
    val cookieExtractorContexts: List[Json] = headerContexts[CookieExtractorEnrichment](
      raw.context.headers,
      registry.cookieExtractor,
      (e, hs) => e.extract(hs)
    )

    // Execute header extractor enrichment
    val httpHeaderExtractorContexts: List[Json] = headerContexts[HttpHeaderExtractorEnrichment](
      raw.context.headers,
      registry.httpHeaderExtractor,
      (e, hs) => e.extract(hs)
    )

    // Fetch weather context
    val weatherContext: F[Either[NonEmptyList[EnrichmentStageIssue], Option[Json]]] =
      getWeatherContext(event, registry.weather)

    val yauaaContext: Option[Json] =
      getYauaaContext(event, registry.yauaa)

    // Validate custom contexts
    val customContexts: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
      Shredder.extractAndValidateCustomContexts(event, client)

    // Validate unstructured event
    val unstructEvent: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
      Shredder.extractAndValidateUnstructEvent(event, client)

    // Assemble array of contexts prepared by built-in enrichments
    val preparedDerivedContexts: F[List[Json]] = for {
      w <- weatherContext
      res = List(uaParser).collect {
        case Right(Some(context)) => context
      } ++ List(w).collect {
        case Right(Some(context)) => context
      } ++ List(iabContext).collect {
        case Right(Some(context)) => context
      } ++ jsScript.getOrElse(Nil) ++
        cookieExtractorContexts ++
        httpHeaderExtractorContexts ++
        yauaaContext
    } yield res

    // Derive some contexts with custom SQL Query enrichment
    val sqlQueryContexts: F[ValidatedNel[EnrichmentStageIssue, List[Json]]] = getSqlQueryContexts(
      event,
      preparedDerivedContexts,
      customContexts,
      unstructEvent,
      registry.sqlQuery
    )

    // Derive some contexts with custom API Request enrichment
    val apiRequestContexts: F[ValidatedNel[EnrichmentStageIssue, List[Json]]] =
      getApiRequestContexts(
        event,
        preparedDerivedContexts,
        customContexts,
        unstructEvent,
        registry.apiRequest
      )

    // Assemble prepared derived contexts with fetched via API Request
    val derivedContexts: F[List[Json]] = for {
      api <- apiRequestContexts
      sql <- sqlQueryContexts
      prepDerivedContexts <- preparedDerivedContexts
    } yield api.getOrElse(Nil) ++ sql.getOrElse(Nil) ++ prepDerivedContexts

    val formatDerivedContexts: F[Unit] = derivedContexts.map { d =>
      if (d.nonEmpty) {
        event.derived_contexts = ME.formatDerivedContexts(d)
      } else {
        ()
      }
    }

    piiTransform(event, registry.piiPseudonymizer)

    // Collect our errors on Failure, or return our event on Success
    (
      customContexts,
      unstructEvent,
      apiRequestContexts,
      sqlQueryContexts,
      extractSchema,
      currency,
      geoLocation(event, registry.ipLookups),
      weatherContext,
      formatDerivedContexts
    ).mapN { (cc, ue, api, sql, es, cu, geo, w, _) =>
      (
        useragent.toValidatedNel,
        collectorTstamp.toValidatedNel,
        derivedTstamp.toValidatedNel,
        uaUtils.toValidatedNel,
        uaParser.toValidatedNel,
        collectorVersionSet.toValidatedNel,
        pageUri.toValidatedNel,
        crossDomain.toValidatedNel,
        geo.asRight.toValidatedNel,
        refererUri.toValidatedNel,
        firstPassTransform,
        cu.toValidated,
        secondPassTransform,
        pageQsMap.toValidatedNel,
        jsScript.toValidatedNel,
        cc,
        ue,
        api,
        sql,
        es.toValidatedNel,
        w.toValidated,
        iabContext.toValidated
      ).mapN((_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => event)
        .leftMap(nel => (nel, EnrichedEvent.toPartiallyEnrichedEvent(event)))
    }
  }

  def setupEnrichedEvent(
    raw: RawEvent,
    etlTstamp: DateTime,
    processor: Processor
  ): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.event_id = EE.generateEventId // May be updated later if we have an `eid` parameter
    e.v_collector = raw.source.name // May be updated later if we have a `cv` parameter
    e.v_etl = ME.etlVersion(processor)
    e.etl_tstamp = EE.toTimestamp(etlTstamp)
    e.network_userid = raw.context.userId.orNull // May be updated later by 'nuid'
    e.user_ipaddress = ME
      .extractIp("user_ipaddress", raw.context.ipAddress.orNull)
      .toOption
      .orNull // May be updated later by 'ip'
    e
  }

  def setCollectorTstamp(
    event: EnrichedEvent,
    timestamp: Option[DateTime]
  ): Either[EnrichmentStageIssue, Unit] =
    EE.formatCollectorTstamp(timestamp).map { t =>
      event.collector_tstamp = t
      ().asRight
    }

  def setUseragent(
    event: EnrichedEvent,
    useragent: Option[String],
    encoding: String
  ): Either[EnrichmentStageIssue, Unit] =
    useragent match {
      case Some(ua) =>
        CU.decodeString(Charset.forName(encoding), ua)
          .map { ua =>
            event.useragent = ua
            ()
          }
          .leftMap(f => EnrichmentFailure(None, SimpleEnrichmentFailureMessage(f)))
      case None => ().asRight // No fields updated
    }

  // The load fails if the collector version is not set
  def getCollectorVersionSet(event: EnrichedEvent): Either[EnrichmentStageIssue, Unit] =
    event.v_collector match {
      case ("" | null) =>
        EnrichmentFailure(
          None,
          InputDataEnrichmentFailureMessage("v_collector", None, "should be set")
        ).asLeft
      case _ => ().asRight
    }

  // If our IpToGeo enrichment is enabled, get the geo-location from the IP address
  // enrichment doesn't fail to maintain the previous approach where failures were suppressed
  // c.f. https://github.com/snowplow/snowplow/issues/351
  def geoLocation[F[_]: Monad](
    event: EnrichedEvent,
    ipLookups: Option[IpLookupsEnrichment[F]]
  ): F[Unit] =
    (for {
      enrichment <- OptionT.fromOption[F](ipLookups)
      ip <- OptionT.fromOption[F](Option(event.user_ipaddress))
      ipLookupResult <- OptionT.liftF(enrichment.extractIpInformation(ip))
      result = {
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
    } yield ()).value.map(_.getOrElse(()))

  // Potentially update the page_url and set the page URL components
  def getPageUri(
    refererUri: Option[String],
    event: EnrichedEvent
  ): Either[EnrichmentStageIssue, Option[URI]] = {
    val pageUri = WPE.extractPageUri(refererUri, Option(event.page_url))
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
    pageUri
  }

  // Calculate the derived timestamp
  def getDerivedTstamp(event: EnrichedEvent): Either[EnrichmentStageIssue, Unit] =
    EE.getDerivedTimestamp(
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
  def getIabContext(
    event: EnrichedEvent,
    iabEnrichment: Option[IabEnrichment]
  ): Either[NonEmptyList[EnrichmentStageIssue], Option[Json]] =
    iabEnrichment match {
      case Some(iab) =>
        event.user_ipaddress match {
          case IPv4Regex(ipv4) if !List(null, "", s"\0").contains(event.useragent) =>
            iab
              .getIabContext(
                Option(event.useragent),
                Option(ipv4),
                Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
              )
              .map(_.some)
          case _ => None.asRight
        }
      case None => None.asRight
    }

  def anonIp(event: EnrichedEvent, anonIp: Option[AnonIpEnrichment]): Option[String] =
    Option(event.user_ipaddress).map { ip =>
      anonIp match {
        case Some(anon) => anon.anonymizeIp(ip)
        case None => ip
      }
    }

  def getUaUtils(
    event: EnrichedEvent,
    userAgentUtils: Option[UserAgentUtilsEnrichment]
  ): Either[EnrichmentStageIssue, Unit] =
    userAgentUtils match {
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
              ()
            }
          case None => ().asRight // No fields updated
        }
      }
      case None => ().asRight
    }

  def getUaParser(
    event: EnrichedEvent,
    uaParser: Option[UaParserEnrichment]
  ): Either[EnrichmentStageIssue, Option[Json]] =
    uaParser match {
      case Some(uap) =>
        Option(event.useragent) match {
          case Some(ua) => uap.extractUserAgent(ua).map(_.some)
          case None => None.asRight // No fields updated
        }
      case None => None.asRight
    }

  def getCurrency[F[_]: Monad](
    event: EnrichedEvent,
    timestamp: Option[DateTime],
    currencyConversion: Option[CurrencyConversionEnrichment[F]]
  ): F[Either[NonEmptyList[EnrichmentStageIssue], Unit]] =
    currencyConversion match {
      case Some(currency) =>
        event.base_currency = currency.baseCurrency.getCode
        // Note that stringToMaybeDouble is applied to either-valid-or-null event POJO
        // properties, so we don't expect any of these four vals to be a Failure
        val trTax = CU.stringToMaybeDouble("tr_tx", event.tr_tax).toValidatedNel
        val tiPrice = CU.stringToMaybeDouble("ti_pr", event.ti_price).toValidatedNel
        val trTotal = CU.stringToMaybeDouble("tr_tt", event.tr_total).toValidatedNel
        val trShipping = CU.stringToMaybeDouble("tr_sh", event.tr_shipping).toValidatedNel
        (for {
          // better-monadic-for
          convertedCu <- EitherT(
            ((trTotal, trTax, trShipping, tiPrice)
              .mapN {
                currency.convertCurrencies(
                  Option(event.tr_currency),
                  _,
                  _,
                  _,
                  Option(event.ti_currency),
                  _,
                  timestamp
                )
              })
              .toEither
              .sequence
              .map(_.flatMap(_.toEither))
          )
          _ = {
            event.tr_total_base = convertedCu._1.orNull
            event.tr_tax_base = convertedCu._2.orNull
            event.tr_shipping_base = convertedCu._3.orNull
            event.ti_price_base = convertedCu._4.orNull
          }
        } yield ()).value
      case None => Monad[F].pure(().asRight)
    }

  def getRefererUri(
    event: EnrichedEvent,
    refererParser: Option[RefererParserEnrichment]
  ): Either[EnrichmentStageIssue, Option[URI]] = {
    val refererUri = CU.stringToUri(event.page_referrer)
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
      refererParser match {
        case Some(rp) =>
          for (refr <- rp.extractRefererDetails(u, event.page_urlhost)) {
            refr match {
              case UnknownReferer(medium) => event.refr_medium = CU.makeTsvSafe(medium.value)
              case SearchReferer(medium, source, term) =>
                event.refr_medium = CU.makeTsvSafe(medium.value)
                event.refr_source = CU.makeTsvSafe(source)
                event.refr_term = CU.makeTsvSafe(term.orNull)
              case InternalReferer(medium) => event.refr_medium = CU.makeTsvSafe(medium.value)
              case SocialReferer(medium, source) =>
                event.refr_medium = CU.makeTsvSafe(medium.value)
                event.refr_source = CU.makeTsvSafe(source)
              case EmailReferer(medium, source) =>
                event.refr_medium = CU.makeTsvSafe(medium.value)
                event.refr_source = CU.makeTsvSafe(source)
              case PaidReferer(medium, source) =>
                event.refr_medium = CU.makeTsvSafe(medium.value)
                event.refr_source = CU.makeTsvSafe(source)
            }
          }
        case None => ().asRight
      }
    }
    refererUri.leftMap(f => EnrichmentFailure(None, SimpleEnrichmentFailureMessage(f)))
  }

  // Parse the page URI's querystring
  def extractQueryString(
    pageUri: Either[EnrichmentStageIssue, Option[URI]],
    encoding: String
  ): Either[EnrichmentStageIssue, Option[Map[String, String]]] = pageUri match {
    case Right(Some(u)) =>
      CU.extractQuerystring(u, Charset.forName(encoding)).map(_.some)
    case _ => None.asRight
  }

  def getCampaign(
    event: EnrichedEvent,
    pageQsMap: Either[EnrichmentStageIssue, Option[Map[String, String]]],
    campaignAttribution: Option[CampaignAttributionEnrichment]
  ): Unit = pageQsMap match {
    case Right(Some(qsMap)) =>
      campaignAttribution match {
        case Some(ce) =>
          val cmp = ce.extractMarketingFields(qsMap)
          event.mkt_medium = CU.makeTsvSafe(cmp.medium.orNull)
          event.mkt_source = CU.makeTsvSafe(cmp.source.orNull)
          event.mkt_term = CU.makeTsvSafe(cmp.term.orNull)
          event.mkt_content = CU.makeTsvSafe(cmp.content.orNull)
          event.mkt_campaign = CU.makeTsvSafe(cmp.campaign.orNull)
          event.mkt_clickid = CU.makeTsvSafe(cmp.clickId.orNull)
          event.mkt_network = CU.makeTsvSafe(cmp.network.orNull)
          ()
        case None => ()
      }
    case _ => ()
  }

  def getCrossDomain(
    event: EnrichedEvent,
    pageQsMap: Either[EnrichmentStageIssue, Option[Map[String, String]]]
  ): Either[EnrichmentStageIssue, Unit] = pageQsMap match {
    case Right(Some(qsMap)) => {
      val crossDomainParseResult = WPE.parseCrossDomain(qsMap)
      for ((maybeRefrDomainUserid, maybeRefrDvceTstamp) <- crossDomainParseResult.toOption) {
        maybeRefrDomainUserid.foreach(event.refr_domain_userid = _)
        maybeRefrDvceTstamp.foreach(event.refr_dvce_tstamp = _)
      }
      crossDomainParseResult.map(_ => ())
    }
    case _ => ().asRight
  }

  def setEventFingerprint(
    event: EnrichedEvent,
    sourceMap: Map[String, String],
    eventFingerprint: Option[EventFingerprintEnrichment]
  ): Unit =
    eventFingerprint match {
      case Some(efe) => event.event_fingerprint = efe.getEventFingerprint(sourceMap)
      case _ => ()
    }

  // Extract the event vendor/name/format/version
  def extractSchemaFields[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[EnrichmentStageIssue, Unit]] =
    SchemaEnrichment
      .extractSchema(event, client)
      .map {
        _.map { schemaKey =>
          event.event_vendor = schemaKey.vendor
          event.event_name = schemaKey.name
          event.event_format = schemaKey.format
          event.event_version = schemaKey.version.asString
          ().asRight
        }
      }

  // Execute the JavaScript scripting enrichment
  def getJsScript(
    event: EnrichedEvent,
    javascriptScript: Option[JavascriptScriptEnrichment]
  ): Either[EnrichmentStageIssue, List[Json]] = javascriptScript match {
    case Some(jse) => jse.process(event)
    case None => Nil.asRight
  }

  def headerContexts[A](
    headers: List[String],
    enrichment: Option[A],
    f: (A, List[String]) => List[Json]
  ): List[Json] = enrichment match {
    case Some(e) => f(e, headers)
    case None => Nil
  }

  // Fetch weather context
  def getWeatherContext[F[_]: Monad](
    event: EnrichedEvent,
    weather: Option[WeatherEnrichment[F]]
  ): F[Either[NonEmptyList[EnrichmentStageIssue], Option[Json]]] = weather match {
    case Some(we) =>
      we.getWeatherContext(
          Option(event.geo_latitude),
          Option(event.geo_longitude),
          Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
        )
        .map(_.map(_.some))
    case None => Monad[F].pure(None.asRight)
  }

  def getYauaaContext(event: EnrichedEvent, yauaa: Option[YauaaEnrichment]): Option[Json] =
    yauaa.map(_.getYauaaContext(event.useragent))

  // Derive some contexts with custom SQL Query enrichment
  def getSqlQueryContexts[F[_]: Monad](
    event: EnrichedEvent,
    preparedDerivedContexts: F[List[Json]],
    customContexts: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]],
    unstructEvent: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]],
    sqlQuery: Option[SqlQueryEnrichment[F]]
  ): F[ValidatedNel[EnrichmentStageIssue, List[Json]]] =
    sqlQuery match {
      case Some(enrichment) =>
        for {
          derivedContexts <- preparedDerivedContexts
          otherContexts <- customContexts.product(unstructEvent)
          lookupResult <- otherContexts match {
            case (Validated.Valid(cctx), Validated.Valid(ue)) =>
              enrichment.lookup(event, derivedContexts, cctx, ue)
            case _ =>
              // Skip. Unstruct event or custom context corrupted (event enrichment will fail)
              Monad[F].pure(Nil.validNel)
          }
        } yield lookupResult
      case None => Monad[F].pure(Nil.validNel)
    }

  // Derive some contexts with custom API Request enrichment
  def getApiRequestContexts[F[_]: Monad](
    event: EnrichedEvent,
    preparedDerivedContexts: F[List[Json]],
    customContexts: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]],
    unstructEvent: F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]],
    apiRequest: Option[ApiRequestEnrichment[F]]
  ): F[ValidatedNel[EnrichmentStageIssue, List[Json]]] =
    apiRequest match {
      case Some(enrichment) =>
        for {
          derivedContexts <- preparedDerivedContexts
          otherContexts <- customContexts.product(unstructEvent)
          lookupResult <- otherContexts match {
            case ((Validated.Valid(cctx), Validated.Valid(ue))) =>
              enrichment.lookup(event, derivedContexts, cctx, ue)
            case _ =>
              // Skip. Unstruct event or custom context corrupted, event enrichment will fail anyway
              Monad[F].pure(Nil.validNel)
          }
        } yield lookupResult
      case None => Monad[F].pure(Nil.validNel)
    }

  def piiTransform(
    event: EnrichedEvent,
    piiPseudonymizer: Option[PiiPseudonymizerEnrichment]
  ): Unit = piiPseudonymizer match {
    case Some(enrichment) => enrichment.transformer(event)
    case None => ()
  }

  // We use a TransformMap which takes the format:
  // "source key" -> (transformFunction, field(s) to set)
  // Caution: by definition, a TransformMap loses type safety. Always unit test!
  private val firstPassTransformMap: TransformMap =
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
      ("vid", (CU.stringToJInteger2, "domain_sessionidx")),
      ("sid", (CU.validateUuid, "domain_sessionid")),
      ("dtm", (EE.extractTimestamp, "dvce_created_tstamp")),
      ("ttm", (EE.extractTimestamp, "true_tstamp")),
      ("stm", (EE.extractTimestamp, "dvce_sent_tstamp")),
      ("tna", (ME.toTsvSafe, "name_tracker")),
      ("tv", (ME.toTsvSafe, "v_tracker")),
      ("cv", (ME.toTsvSafe, "v_collector")),
      ("lang", (ME.toTsvSafe, "br_lang")),
      ("f_pdf", (CU.stringToBooleanLikeJByte, "br_features_pdf")),
      ("f_fla", (CU.stringToBooleanLikeJByte, "br_features_flash")),
      ("f_java", (CU.stringToBooleanLikeJByte, "br_features_java")),
      ("f_dir", (CU.stringToBooleanLikeJByte, "br_features_director")),
      ("f_qt", (CU.stringToBooleanLikeJByte, "br_features_quicktime")),
      ("f_realp", (CU.stringToBooleanLikeJByte, "br_features_realplayer")),
      ("f_wma", (CU.stringToBooleanLikeJByte, "br_features_windowsmedia")),
      ("f_gears", (CU.stringToBooleanLikeJByte, "br_features_gears")),
      ("f_ag", (CU.stringToBooleanLikeJByte, "br_features_silverlight")),
      ("cookie", (CU.stringToBooleanLikeJByte, "br_cookies")),
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
      ("ev_va", (CU.stringToDoubleLike, "se_value")), // LEGACY tracker var. Leave for backwards compat
      ("se_ca", (ME.toTsvSafe, "se_category")),
      ("se_ac", (ME.toTsvSafe, "se_action")),
      ("se_la", (ME.toTsvSafe, "se_label")),
      ("se_pr", (ME.toTsvSafe, "se_property")),
      ("se_va", (CU.stringToDoubleLike, "se_value")),
      // Custom unstructured events
      ("ue_pr", (JU.extractUnencJson, "unstruct_event")),
      ("ue_px", (JU.extractBase64EncJson, "unstruct_event")),
      // Ecommerce transactions
      ("tr_id", (ME.toTsvSafe, "tr_orderid")),
      ("tr_af", (ME.toTsvSafe, "tr_affiliation")),
      ("tr_tt", (CU.stringToDoubleLike, "tr_total")),
      ("tr_tx", (CU.stringToDoubleLike, "tr_tax")),
      ("tr_sh", (CU.stringToDoubleLike, "tr_shipping")),
      ("tr_ci", (ME.toTsvSafe, "tr_city")),
      ("tr_st", (ME.toTsvSafe, "tr_state")),
      ("tr_co", (ME.toTsvSafe, "tr_country")),
      // Ecommerce transaction items
      ("ti_id", (ME.toTsvSafe, "ti_orderid")),
      ("ti_sk", (ME.toTsvSafe, "ti_sku")),
      ("ti_na", (ME.toTsvSafe, "ti_name")), // ERROR in Tracker Protocol
      ("ti_nm", (ME.toTsvSafe, "ti_name")),
      ("ti_ca", (ME.toTsvSafe, "ti_category")),
      ("ti_pr", (CU.stringToDoubleLike, "ti_price")),
      ("ti_qu", (CU.stringToJInteger2, "ti_quantity")),
      // Page pings
      ("pp_mix", (CU.stringToJInteger2, "pp_xoffset_min")),
      ("pp_max", (CU.stringToJInteger2, "pp_xoffset_max")),
      ("pp_miy", (CU.stringToJInteger2, "pp_yoffset_min")),
      ("pp_may", (CU.stringToJInteger2, "pp_yoffset_max")),
      // Currency
      ("tr_cu", (ME.toTsvSafe, "tr_currency")),
      ("ti_cu", (ME.toTsvSafe, "ti_currency"))
    )

  // A second TransformMap which can overwrite values set by the first
  private val secondPassTransformMap: TransformMap =
    // Overwrite collector-set nuid with tracker-set tnuid
    Map(("tnuid", (ME.toTsvSafe, "network_userid")))
}
