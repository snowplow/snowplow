/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import scala.util.matching.Regex

import java.nio.charset.Charset
import java.net.URI
import java.time.Instant

import org.joda.time.DateTime

import io.circe.Json

import cats.Monad
import cats.data.{EitherT, NonEmptyList, OptionT, ValidatedNel}
import cats.effect.Clock
import cats.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Payload, Processor}

import com.snowplowanalytics.refererparser._

import adapters.RawEvent
import enrichments.{EventEnrichments => EE}
import enrichments.{MiscEnrichments => ME}
import enrichments.registry._
import enrichments.registry.apirequest.ApiRequestEnrichment
import enrichments.registry.pii.PiiPseudonymizerEnrichment
import enrichments.registry.sqlquery.SqlQueryEnrichment
import enrichments.web.{PageEnrichments => WPE}
import outputs.EnrichedEvent
import utils.{IgluUtils, ConversionUtils => CU}

object EnrichmentManager {

  // Regex for IPv4 without port
  val IPv4Regex: Regex = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*""".r

  /** Run the enrichment workflow
   * @param registry Contain configuration for all enrichments to apply
   * @param client Iglu Client, for schema lookups and validation
   * @param processor Meta information about processing asset, for bad rows
   * @param etlTstamp ETL timestamp
   * @param raw Canonical input event to enrich
   * @return Enriched event or bad row if a problem occured
   */
  def enrichEvent[F[_]: Monad: RegistryLookup: Clock](
    registry: EnrichmentRegistry[F],
    client: Client[F, Json],
    processor: Processor,
    etlTstamp: DateTime,
    raw: RawEvent
  ): EitherT[F, BadRow, EnrichedEvent] =
    for {
      enriched <- EitherT.rightT[F, BadRow](setupEnrichedEvent(raw, etlTstamp, processor))
      _ <- EitherT.fromEither[F](Transform.transform(raw, enriched, processor))
      inputSDJs <- IgluUtils.extractAndValidateInputJsons(enriched, client, raw, processor)
      (inputContexts, unstructEvent) = inputSDJs
      enrichmentsContexts <- runEnrichments(
        registry,
        processor,
        raw,
        enriched,
        inputContexts,
        unstructEvent
      )
      _ <- IgluUtils
        .validateEnrichmentsContexts[F](client, enrichmentsContexts, raw, processor, enriched)
      _ <- EitherT.rightT[F, BadRow] {
        if (enrichmentsContexts.nonEmpty)
          enriched.derived_contexts = ME.formatDerivedContexts(enrichmentsContexts)
      }
      _ <- EitherT.rightT[F, BadRow](
        anonIp(enriched, registry.anonIp).foreach(enriched.user_ipaddress = _)
      )
      _ <- EitherT.rightT[F, BadRow] {
        piiTransform(enriched, registry.piiPseudonymizer).foreach { pii =>
          enriched.pii = pii.asString
        }
      }
    } yield enriched

  /** Run all the enrichments and aggregate the errors if any
   * @param enriched /!\ MUTABLE enriched event, mutated IN-PLACE /!\
   * @return List of contexts to attach to the enriched event if all the enrichments went well
   *         or [[BadRow.EnrichmentFailures]] if something wrong happened
   *         with at least one enrichment
   */
  private def runEnrichments[F[_]: Monad](
    registry: EnrichmentRegistry[F],
    processor: Processor,
    raw: RawEvent,
    enriched: EnrichedEvent,
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]]
  ): EitherT[F, BadRow.EnrichmentFailures, List[SelfDescribingData[Json]]] = EitherT {
    // Validate that the collectorTstamp exists and is Redshift-compatible
    val collectorTstamp: Either[FailureDetails.EnrichmentFailure, Unit] =
      setCollectorTstamp(enriched, raw.context.timestamp)

    // Attempt to decode the useragent
    // May be updated later if we have a `ua` parameter
    val useragent: Either[FailureDetails.EnrichmentFailure, Unit] =
      setUseragent(enriched, raw.context.useragent, raw.source.encoding)

    // The load fails if the collector version is not set
    val collectorVersionSet: Either[FailureDetails.EnrichmentFailure, Unit] =
      getCollectorVersionSet(enriched)

    // Potentially update the page_url and set the page URL components
    val pageUri: Either[FailureDetails.EnrichmentFailure, Option[URI]] =
      getPageUri(raw.context.refererUri, enriched)

    // Calculate the derived timestamp
    val derivedTstamp: Either[FailureDetails.EnrichmentFailure, Unit] =
      getDerivedTstamp(enriched)

    // Fetch IAB enrichment context (before anonymizing the IP address)
    val iabContext: Either[NonEmptyList[FailureDetails.EnrichmentFailure], Option[
      SelfDescribingData[Json]
    ]] =
      getIabContext(enriched, registry.iab)

    // Parse the useragent using user-agent-utils
    val uaUtils: Either[FailureDetails.EnrichmentFailure, Unit] =
      getUaUtils(enriched, registry.userAgentUtils)

    // Create the ua_parser_context
    val uaParser: Either[FailureDetails.EnrichmentFailure, Option[SelfDescribingData[Json]]] =
      getUaParser(enriched, registry.uaParser)

    // Finalize the currency conversion
    val currency: F[Either[NonEmptyList[FailureDetails.EnrichmentFailure], Unit]] =
      getCurrency(enriched, raw.context.timestamp, registry.currencyConversion)

    // Potentially set the referrer details and URL components
    val refererUri: Either[FailureDetails.EnrichmentFailure, Option[URI]] =
      getRefererUri(enriched, registry.refererParser)

    // Parse the page URI's querystring
    val pageQsMap: Either[FailureDetails.EnrichmentFailure, Option[Map[String, String]]] =
      extractQueryString(pageUri, raw.source.encoding)

    // Marketing attribution
    setCampaign(enriched, pageQsMap, registry.campaignAttribution)

    // Cross-domain tracking
    val crossDomain: Either[FailureDetails.EnrichmentFailure, Unit] =
      getCrossDomain(enriched, pageQsMap)

    // This enrichment cannot fail
    setEventFingerprint(enriched, raw.parameters, registry.eventFingerprint)

    // Execute cookie extractor enrichment
    val cookieExtractorContexts: List[SelfDescribingData[Json]] =
      headerContexts[CookieExtractorEnrichment](
        raw.context.headers,
        registry.cookieExtractor,
        (e, hs) => e.extract(hs)
      )

    // Execute header extractor enrichment
    val httpHeaderExtractorContexts: List[SelfDescribingData[Json]] =
      headerContexts[HttpHeaderExtractorEnrichment](
        raw.context.headers,
        registry.httpHeaderExtractor,
        (e, hs) => e.extract(hs)
      )

    // Fetch weather context
    val weatherContext: F[
      Either[NonEmptyList[FailureDetails.EnrichmentFailure], Option[SelfDescribingData[Json]]]
    ] =
      getWeatherContext(enriched, registry.weather)

    // Runs YAUAA enrichment (gets info thanks to user agent)
    val yauaaContext: Option[SelfDescribingData[Json]] =
      getYauaaContext(enriched, registry.yauaa)

    // Extract the event vendor/name/format/version
    val extractSchema: Either[FailureDetails.EnrichmentFailure, Unit] =
      extractSchemaFields(enriched, unstructEvent)

    // Execute IP lookup enrichment
    val geoloc = geoLocation(enriched, registry.ipLookups)

    // Execute the JavaScript scripting enrichment
    val jsScript: Either[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]] =
      getJsScript(enriched, registry.javascriptScript)

    // Enrichments that don't run in a Monad
    val noMonadEnrichments: ValidatedNel[FailureDetails.EnrichmentFailure, Unit] =
      List(
        collectorTstamp.toValidatedNel,
        useragent.toValidatedNel,
        collectorVersionSet.toValidatedNel,
        pageUri.toValidatedNel,
        derivedTstamp.toValidatedNel,
        iabContext.toValidated,
        uaUtils.toValidatedNel,
        uaParser.toValidatedNel,
        refererUri.toValidatedNel,
        pageQsMap.toValidatedNel,
        crossDomain.toValidatedNel,
        extractSchema.toValidatedNel,
        jsScript.toValidatedNel
      ).sequence_

    // Contexts of built-in enrichments
    val builtInContexts: F[List[SelfDescribingData[Json]]] =
      for {
        w <- weatherContext
        res = List(w).collect {
          case Right(Some(context)) => context
        } ++ List(uaParser).collect {
          case Right(Some(context)) => context
        } ++ List(iabContext).collect {
          case Right(Some(context)) => context
        } ++ List(yauaaContext).collect {
          case Some(context) => context
        } ++ jsScript.getOrElse(Nil) ++
          cookieExtractorContexts ++
          httpHeaderExtractorContexts
      } yield res

    // Derive some contexts with custom SQL Query enrichment
    val sqlQueryContexts: F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] =
      getSqlQueryContexts[F](
        enriched,
        builtInContexts,
        inputContexts,
        unstructEvent,
        registry.sqlQuery
      )

    // Derive some contexts with custom API Request enrichment
    val apiRequestContexts: F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] =
      getApiRequestContexts[F](
        enriched,
        builtInContexts,
        inputContexts,
        unstructEvent,
        registry.apiRequest
      )

    // Merge all enrichments errors if any
    (
      currency,
      weatherContext,
      geoloc,
      sqlQueryContexts,
      apiRequestContexts,
      builtInContexts
    ).mapN { (cur, wea, _, sql, api, ctxts) =>
      (
        cur.toValidated,
        wea.toValidated,
        sql,
        api,
        noMonadEnrichments
      ).mapN { (_, _, sqlContexts, apiContexts, _) =>
          ctxts ++ sqlContexts ++ apiContexts
        }
        .leftMap(
          enrichmentFailures =>
            buildEnrichmentFailuresBadRow(
              enrichmentFailures,
              EnrichedEvent.toPartiallyEnrichedEvent(enriched),
              RawEvent.toRawEvent(raw),
              processor
            )
        )
        .toEither
    }
  }

  /** Create the mutable [[EnrichedEvent]] and initialize it. */
  private def setupEnrichedEvent(
    raw: RawEvent,
    etlTstamp: DateTime,
    processor: Processor
  ): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.event_id = EE.generateEventId() // May be updated later if we have an `eid` parameter
    e.v_collector = raw.source.name // May be updated later if we have a `cv` parameter
    e.v_etl = ME.etlVersion(processor)
    e.etl_tstamp = EE.toTimestamp(etlTstamp)
    e.network_userid = raw.context.userId.map(_.toString).orNull // May be updated later by 'nuid'
    e.user_ipaddress = ME
      .extractIp("user_ipaddress", raw.context.ipAddress.orNull)
      .toOption
      .orNull // May be updated later by 'ip'
    e
  }

  def setCollectorTstamp(event: EnrichedEvent, timestamp: Option[DateTime]): Either[FailureDetails.EnrichmentFailure, Unit] =
    EE.formatCollectorTstamp(timestamp).map { t =>
      event.collector_tstamp = t
      ().asRight
    }

  def setUseragent(
    event: EnrichedEvent,
    useragent: Option[String],
    encoding: String
  ): Either[FailureDetails.EnrichmentFailure, Unit] =
    useragent match {
      case Some(ua) =>
        CU.decodeString(Charset.forName(encoding), ua)
          .map { ua =>
            event.useragent = ua
            ()
          }
          .leftMap(
            f =>
              FailureDetails.EnrichmentFailure(
                None,
                FailureDetails.EnrichmentFailureMessage.Simple(f)
              )
          )
      case None => ().asRight // No fields updated
    }

  // The load fails if the collector version is not set
  def getCollectorVersionSet(event: EnrichedEvent): Either[FailureDetails.EnrichmentFailure, Unit] =
    event.v_collector match {
      case "" | null =>
        FailureDetails
          .EnrichmentFailure(
            None,
            FailureDetails.EnrichmentFailureMessage
              .InputData("v_collector", None, "should be set")
          )
          .asLeft
      case _ => ().asRight
    }

  // If our IpToGeo enrichment is enabled, get the geo-location from the IP address
  // enrichment doesn't fail to maintain the previous approach where failures were suppressed
  // c.f. https://github.com/snowplow/snowplow/issues/351
  def geoLocation[F[_]: Monad](event: EnrichedEvent, ipLookups: Option[IpLookupsEnrichment[F]]): F[Unit] = {
    val ipLookup = for {
      enrichment <- OptionT.fromOption[F](ipLookups)
      ip <- OptionT.fromOption[F](Option(event.user_ipaddress))
      result <- OptionT.liftF(enrichment.extractIpInformation(ip))
    } yield result

    ipLookup.value.map {
      case Some(lookup) =>
        lookup.ipLocation.flatMap(_.toOption).foreach { loc =>
          event.geo_country = loc.countryCode
          event.geo_region = loc.region.orNull
          event.geo_city = loc.city.orNull
          event.geo_zipcode = loc.postalCode.orNull
          event.geo_latitude = loc.latitude
          event.geo_longitude = loc.longitude
          event.geo_region_name = loc.regionName.orNull
          event.geo_timezone = loc.timezone.orNull
        }
        lookup.isp.flatMap(_.toOption).foreach { i =>
          event.ip_isp = i
        }
        lookup.organization.flatMap(_.toOption).foreach { org =>
          event.ip_organization = org
        }
        lookup.domain.flatMap(_.toOption).foreach { d =>
          event.ip_domain = d
        }
        lookup.connectionType.flatMap(_.toOption).foreach { ct =>
          event.ip_netspeed = ct
        }
      case None =>
        ()
    }

  }

  // Potentially update the page_url and set the page URL components
  def getPageUri(refererUri: Option[String], event: EnrichedEvent): Either[FailureDetails.EnrichmentFailure, Option[URI]] = {
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
  def getDerivedTstamp(event: EnrichedEvent): Either[FailureDetails.EnrichmentFailure, Unit] =
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
  ): Either[NonEmptyList[FailureDetails.EnrichmentFailure], Option[SelfDescribingData[Json]]] =
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

  def getUaUtils(event: EnrichedEvent, userAgentUtils: Option[UserAgentUtilsEnrichment]): Either[FailureDetails.EnrichmentFailure, Unit] =
    userAgentUtils match {
      case Some(uap) =>
        Option(event.useragent) match {
          case Some(ua) =>
            val ca = uap.extractClientAttributes(ua)
            ca.map { c =>
              event.br_name = c.browserName
              event.br_family = c.browserFamily
              c.browserVersion.foreach(bv => event.br_version = bv)
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
      case None => ().asRight
    }

  def getUaParser(
    event: EnrichedEvent,
    uaParser: Option[UaParserEnrichment]
  ): Either[FailureDetails.EnrichmentFailure, Option[SelfDescribingData[Json]]] =
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
  ): F[Either[NonEmptyList[FailureDetails.EnrichmentFailure], Unit]] =
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
            (trTotal, trTax, trShipping, tiPrice)
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
              }
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
  ): Either[FailureDetails.EnrichmentFailure, Option[URI]] = {
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
    refererUri.leftMap(
      f =>
        FailureDetails.EnrichmentFailure(
          None,
          FailureDetails.EnrichmentFailureMessage.Simple(f)
        )
    )
  }

  // Parse the page URI's querystring
  def extractQueryString(
    pageUri: Either[FailureDetails.EnrichmentFailure, Option[URI]],
    encoding: String
  ): Either[FailureDetails.EnrichmentFailure, Option[Map[String, String]]] = pageUri match {
    case Right(Some(u)) =>
      CU.extractQuerystring(u, Charset.forName(encoding)).map(_.some)
    case _ => None.asRight
  }

  def setCampaign(
    event: EnrichedEvent,
    pageQsMap: Either[FailureDetails.EnrichmentFailure, Option[Map[String, String]]],
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
    pageQsMap: Either[FailureDetails.EnrichmentFailure, Option[Map[String, String]]]
  ): Either[FailureDetails.EnrichmentFailure, Unit] = pageQsMap match {
    case Right(Some(qsMap)) =>
      val crossDomainParseResult = WPE.parseCrossDomain(qsMap)
      for ((maybeRefrDomainUserid, maybeRefrDvceTstamp) <- crossDomainParseResult.toOption) {
        maybeRefrDomainUserid.foreach(event.refr_domain_userid = _)
        maybeRefrDvceTstamp.foreach(event.refr_dvce_tstamp = _)
      }
      crossDomainParseResult.map(_ => ())
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
  def extractSchemaFields(
    event: EnrichedEvent,
    unstructEvent: Option[SelfDescribingData[Json]]
  ): Either[FailureDetails.EnrichmentFailure, Unit] =
    SchemaEnrichment
      .extractSchema(event, unstructEvent)
      .map {
        case Some(schemaKey) =>
          event.event_vendor = schemaKey.vendor
          event.event_name = schemaKey.name
          event.event_format = schemaKey.format
          event.event_version = schemaKey.version.asString
          ()
        case None => ()
      }

  // Execute the JavaScript scripting enrichment
  def getJsScript(
    event: EnrichedEvent,
    javascriptScript: Option[JavascriptScriptEnrichment]
  ): Either[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]] =
    javascriptScript match {
      case Some(jse) => jse.process(event)
      case None => Nil.asRight
    }

  def headerContexts[A](
    headers: List[String],
    enrichment: Option[A],
    f: (A, List[String]) => List[SelfDescribingData[Json]]
  ): List[SelfDescribingData[Json]] = enrichment match {
    case Some(e) => f(e, headers)
    case None => Nil
  }

  // Fetch weather context
  def getWeatherContext[F[_]: Monad](event: EnrichedEvent, weather: Option[WeatherEnrichment[F]]): F[
    Either[NonEmptyList[FailureDetails.EnrichmentFailure], Option[SelfDescribingData[Json]]]
  ] =
    weather match {
      case Some(we) =>
        we.getWeatherContext(
            Option(event.geo_latitude),
            Option(event.geo_longitude),
            Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
          )
          .map(_.map(_.some))
      case None => Monad[F].pure(None.asRight)
    }

  def getYauaaContext(event: EnrichedEvent, yauaa: Option[YauaaEnrichment]): Option[SelfDescribingData[Json]] =
    yauaa.map(_.getYauaaContext(event.useragent))

  // Derive some contexts with custom SQL Query enrichment
  def getSqlQueryContexts[F[_]: Monad](
    event: EnrichedEvent,
    enrichmentsContexts: F[List[SelfDescribingData[Json]]],
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    sqlQuery: Option[SqlQueryEnrichment[F]]
  ): F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] =
    sqlQuery match {
      case Some(enrichment) =>
        for {
          derivedContexts <- enrichmentsContexts
          lookupResult <- enrichment.lookup(event, derivedContexts, inputContexts, unstructEvent)
        } yield lookupResult
      case None =>
        List.empty[SelfDescribingData[Json]].validNel[FailureDetails.EnrichmentFailure].pure[F]
    }

  // Derive some contexts with custom API Request enrichment
  def getApiRequestContexts[F[_]: Monad](
    event: EnrichedEvent,
    enrichmentsContexts: F[List[SelfDescribingData[Json]]],
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    apiRequest: Option[ApiRequestEnrichment[F]]
  ): F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] =
    apiRequest match {
      case Some(enrichment) =>
        for {
          derivedContexts <- enrichmentsContexts
          lookupResult <- enrichment.lookup(event, derivedContexts, inputContexts, unstructEvent)
        } yield lookupResult
      case None =>
        List.empty[SelfDescribingData[Json]].validNel[FailureDetails.EnrichmentFailure].pure[F]
    }

  def piiTransform(event: EnrichedEvent, piiPseudonymizer: Option[PiiPseudonymizerEnrichment]): Option[SelfDescribingData[Json]] =
    piiPseudonymizer.flatMap(_.transformer(event))

  /** Build `BadRow.EnrichmentFailures` from a list of `FailureDetails.EnrichmentFailure`s */
  def buildEnrichmentFailuresBadRow(
    fs: NonEmptyList[FailureDetails.EnrichmentFailure],
    pee: Payload.PartiallyEnrichedEvent,
    re: Payload.RawEvent,
    processor: Processor
  ) = BadRow.EnrichmentFailures(
    processor,
    Failure.EnrichmentFailures(Instant.now(), fs),
    Payload.EnrichmentPayload(pee, re)
  )
}
