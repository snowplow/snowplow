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
package adapters

import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList, Validated}
import cats.effect.Clock
import cats.syntax.functor._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows._
import io.circe.Json

import loaders.CollectorPayload
import registry._
import registry.snowplow._
import utils.HttpClient

/**
 * The AdapterRegistry lets us convert a CollectorPayload into one or more RawEvents, using a given
 * adapter.
 */
class AdapterRegistry(remoteAdapters: Map[(String, String), RemoteAdapter] = Map.empty) {

  val adapters: Map[(String, String), Adapter] = Map(
    (Vendor.Snowplow, "tp1") -> Tp1Adapter,
    (Vendor.Snowplow, "tp2") -> Tp2Adapter,
    (Vendor.Redirect, "tp2") -> RedirectAdapter,
    (Vendor.Iglu, "v1") -> IgluAdapter,
    (Vendor.Callrail, "v1") -> CallrailAdapter,
    (Vendor.Cloudfront, "wd_access_log") -> CloudfrontAccessLogAdapter,
    (Vendor.Mailchimp, "v1") -> MailchimpAdapter,
    (Vendor.Mailgun, "v1") -> MailgunAdapter,
    (Vendor.GoogleAnalytics, "v1") -> GoogleAnalyticsAdapter,
    (Vendor.Mandrill, "v1") -> MandrillAdapter,
    (Vendor.Olark, "v1") -> OlarkAdapter,
    (Vendor.Pagerduty, "v1") -> PagerdutyAdapter,
    (Vendor.Pingdom, "v1") -> PingdomAdapter,
    (Vendor.Sendgrid, "v3") -> SendgridAdapter,
    (Vendor.StatusGator, "v1") -> StatusGatorAdapter,
    (Vendor.Unbounce, "v1") -> UnbounceAdapter,
    (Vendor.UrbanAirship, "v1") -> UrbanAirshipAdapter,
    (Vendor.Marketo, "v1") -> MarketoAdapter,
    (Vendor.Vero, "v1") -> VeroAdapter,
    (Vendor.HubSpot, "v1") -> HubSpotAdapter
  ) ++ remoteAdapters

  private object Vendor {
    val Snowplow = "com.snowplowanalytics.snowplow"
    val Redirect = "r"
    val Iglu = "com.snowplowanalytics.iglu"
    val Callrail = "com.callrail"
    val Cloudfront = "com.amazon.aws.cloudfront"
    val GoogleAnalytics = "com.google.analytics"
    val Mailchimp = "com.mailchimp"
    val Mailgun = "com.mailgun"
    val Mandrill = "com.mandrill"
    val Olark = "com.olark"
    val Pagerduty = "com.pagerduty"
    val Pingdom = "com.pingdom"
    val Sendgrid = "com.sendgrid"
    val StatusGator = "com.statusgator"
    val Unbounce = "com.unbounce"
    val UrbanAirship = "com.urbanairship.connect"
    val Marketo = "com.marketo"
    val Vero = "com.getvero"
    val HubSpot = "com.hubspot"
  }

  /**
   * Router to determine which adapter we use to convert the CollectorPayload into one or more
   * RawEvents.
   * @param payload The CollectorPayload we are transforming
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Strings on
   * Failure
   */
  def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](
    payload: CollectorPayload,
    client: Client[F, Json],
    processor: Processor
  ): F[Validated[BadRow, NonEmptyList[RawEvent]]] =
    (adapters.get((payload.api.vendor, payload.api.version)) match {
      case Some(adapter) => adapter.toRawEvents(payload, client)
      case _ =>
        val f = FailureDetails.AdapterFailure.InputData(
          "vendor/version",
          Some(s"${payload.api.vendor}/${payload.api.version}"),
          "vendor/version combination is not supported"
        )
        Monad[F].pure(f.invalidNel)
    }).map(_.leftMap(enrichFailure(_, payload, payload.api.vendor, payload.api.version, processor)))

  private def enrichFailure(
    fs: NonEmptyList[FailureDetails.AdapterFailureOrTrackerProtocolViolation],
    cp: CollectorPayload,
    vendor: String,
    vendorVersion: String,
    processor: Processor
  ): BadRow = {
    val payload = cp.toBadRowPayload
    if (vendorVersion == "tp2" && (vendor == Vendor.Snowplow || vendor == Vendor.Redirect)) {
      val tpViolations = fs.asInstanceOf[NonEmptyList[FailureDetails.TrackerProtocolViolation]]
      val failure =
        Failure.TrackerProtocolViolations(Instant.now(), vendor, vendorVersion, tpViolations)
      BadRow.TrackerProtocolViolations(processor, failure, payload)
    } else {
      val adapterFailures = fs.asInstanceOf[NonEmptyList[FailureDetails.AdapterFailure]]
      val failure = Failure.AdapterFailures(Instant.now(), vendor, vendorVersion, adapterFailures)
      BadRow.AdapterFailures(processor, failure, payload)
    }
  }
}
