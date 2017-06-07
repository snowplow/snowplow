/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// Snowplow
import loaders.{CollectorApi, CollectorSource, CollectorContext, CollectorPayload}

class MeasurementProtocolAdapterSpec extends Specification with DataTables with ValidationMatchers {
  def is = s2"""
    This is a specification to test the MeasurementProtocolAdapter functionality
    toRawEvents must return a failNel if the query string is empty               $e1
    toRawEvents must return a failNel if there is no t param in the query string $e2
    toRawEvents must return a failNel if there are no corresponding hit types    $e3
    toRawEvents must return a succNel if the payload is correct and complete     $e4
    toRawEvents must return a succNel if the payload is correct and partial      $e5
  """

  implicit val resolver = SpecHelpers.IgluResolver

  val api = CollectorApi("com.google.analytics.measurement-protocol", "v1")
  val source = CollectorSource("clj-tomcat", "UTF-8", None)
  val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
    "37.157.33.123".some, None, None, Nil, None)

  val static = Map(
    "tv" -> "com.google.analytics.measurement-protocol-v1",
    "e"  -> "ue",
    "p" -> "srv"
  )

  def e1 = {
    val params = SpecHelpers.toNameValuePairs()
    val payload = CollectorPayload(api, params, None, None, source, context)
    val actual = MeasurementProtocolAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(
      "Querystring is empty: no MeasurementProtocol event to process"))
  }

  def e2 = {
    val params = SpecHelpers.toNameValuePairs("dl" -> "document location")
    val payload = CollectorPayload(api, params, None, None, source, context)
    val actual = MeasurementProtocolAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(
      "No MeasurementProtocol t parameter provided: cannot determine hit type"))
  }

  def e3 = {
    val params = SpecHelpers.toNameValuePairs("t" -> "unknown", "dl" -> "document location")
    val payload = CollectorPayload(api, params, None, None, source, context)
    val actual = MeasurementProtocolAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(
      "No matching MeasurementProtocol hit type for hit type unknown"))
  }

  def e4 = {
    val params = SpecHelpers.toNameValuePairs(
      "t"  -> "pageview",
      "dl" -> "document location",
      "dh" -> "host name",
      "dp" -> "path",
      "dt" -> "title"
    )
    val payload = CollectorPayload(api, params, None, None, source, context)
    val actual = MeasurementProtocolAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"documentLocationURL":"document location",
               |"documentTitle":"title",
               |"documentPath":"path",
               |"hitType":"pageview",
               |"documentHostName":"host name"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedJson)
    actual must beSuccessful(NonEmptyList(RawEvent(api, expectedParams, None, source, context)))
  }

  def e5 = {
    val params = SpecHelpers.toNameValuePairs("t" -> "pageview", "dl" -> "document location")
    val payload = CollectorPayload(api, params, None, None, source, context)
    val actual = MeasurementProtocolAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"hitType":"pageview",
               |"documentLocationURL":"document location"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedJson)
    actual must beSuccessful(NonEmptyList(RawEvent(api, expectedParams, None, source, context)))
  }
}