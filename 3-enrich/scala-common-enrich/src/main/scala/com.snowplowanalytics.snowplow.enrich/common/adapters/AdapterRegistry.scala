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
package com.snowplowanalytics 
package snowplow
package enrich
package common
package adapters

// Iglu
import iglu.client.Resolver

// Scalaz
import scalaz._
import Scalaz._

// This project
import loaders.CollectorPayload
import registry.snowplow.{Tp1Adapter => SpTp1Adapter}
import registry.snowplow.{Tp2Adapter => SpTp2Adapter}
import registry.snowplow.{RedirectAdapter => SpRedirectAdapter}
import registry._

/**
 * The AdapterRegistry lets us convert a CollectorPayload
 * into one or more RawEvents, using a given adapter.
 */
object AdapterRegistry {

  private object Vendor {
    val Snowplow            = "com.snowplowanalytics.snowplow"
    val Redirect            = "r"
    val Iglu                = "com.snowplowanalytics.iglu"
    val Callrail            = "com.callrail"
    val Mailchimp           = "com.mailchimp"
    val Mandrill            = "com.mandrill"
    val Pagerduty           = "com.pagerduty"
    val Pingdom             = "com.pingdom"
    val Cloudfront          = "com.amazon.aws.cloudfront"
    val UrbanAirship        = "com.urbanairship.connect"
    val Sendgrid            = "com.sendgrid"
    val MeasurementProtocol = "com.google.analytics.measurement-protocol"
  }

  /**
   * Router to determine which adapter we use
   * to convert the CollectorPayload into
   * one or more RawEvents.
   *
   * @param payload The CollectorPayload we
   *        are transforming
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return a Validation boxing either a
   *         NEL of RawEvents on Success,
   *         or a NEL of Strings on Failure
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = (payload.api.vendor, payload.api.version) match {
    case (Vendor.Snowplow,             "tp1") => SpTp1Adapter.toRawEvents(payload)
    case (Vendor.Snowplow,             "tp2") => SpTp2Adapter.toRawEvents(payload)
    case (Vendor.Redirect,             "tp2") => SpRedirectAdapter.toRawEvents(payload)
    case (Vendor.Iglu,                  "v1") => IgluAdapter.toRawEvents(payload)
    case (Vendor.Callrail,              "v1") => CallrailAdapter.toRawEvents(payload)
    case (Vendor.Mailchimp,             "v1") => MailchimpAdapter.toRawEvents(payload)
    case (Vendor.Mandrill,              "v1") => MandrillAdapter.toRawEvents(payload)
    case (Vendor.Pagerduty,             "v1") => PagerdutyAdapter.toRawEvents(payload)
    case (Vendor.Pingdom,               "v1") => PingdomAdapter.toRawEvents(payload)
    case (Vendor.Cloudfront, "wd_access_log") => CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(payload)
    case (Vendor.UrbanAirship,          "v1") => UrbanAirshipAdapter.toRawEvents(payload)
    case (Vendor.Sendgrid,              "v3") => SendgridAdapter.toRawEvents(payload)
    case (Vendor.MeasurementProtocol,   "v1") => MeasurementProtocolAdapter.toRawEvents(payload)
    case _ => s"Payload with vendor ${payload.api.vendor} and version ${payload.api.version} not supported by this version of Scala Common Enrich".failNel
  }

}
