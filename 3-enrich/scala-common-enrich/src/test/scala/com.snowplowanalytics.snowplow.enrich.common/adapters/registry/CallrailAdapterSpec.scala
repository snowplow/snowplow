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
package registry

import cats.Eval
import cats.data.NonEmptyList
import cats.syntax.option._
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup._
import com.snowplowanalytics.snowplow.badrows._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import SpecHelpers._
import utils.Clock._

class CallrailAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  toRawEvents should return a NEL containing one RawEvent if the querystring is correctly populated $e1
  toRawEvents should return a Validation Failure if there are no parameters on the querystring      $e2
  """

  object Shared {
    val api = CollectorPayload.Api("com.callrail", "v1")
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.callrail-v1",
      "e" -> "ue",
      "cv" -> "clj-0.6.0-tom-0.0.4"
    )
    val static = staticNoPlatform + ("p" -> "srv")
  }

  def e1 = {
    val params = toNameValuePairs(
      "answered" -> "true",
      "callercity" -> "BAKERSFIELD",
      "callercountry" -> "US",
      "callername" -> "SKYPE CALLER",
      "callernum" -> "+12612230240",
      "callerstate" -> "CA",
      "callerzip" -> "92307",
      "callsource" -> "keyword",
      "datetime" -> "2014-10-09 16:23:45",
      "destinationnum" -> "2012032051",
      "duration" -> "247",
      "first_call" -> "true",
      "ga" -> "",
      "gclid" -> "",
      "id" -> "201235151",
      "ip" -> "86.178.163.7",
      "keywords" -> "",
      "kissmetrics_id" -> "",
      "landingpage" -> "http://acme.com/",
      "recording" -> "http://app.callrail.com/calls/201235151/recording/9f59ad59ba1cfa264312",
      "referrer" -> "direct",
      "referrermedium" -> "Direct",
      "trackingnum" -> "+12012311668",
      "transcription" -> "",
      "utm_campaign" -> "",
      "utm_content" -> "",
      "utm_medium" -> "",
      "utm_source" -> "",
      "utm_term" -> "",
      "utma" -> "",
      "utmb" -> "",
      "utmc" -> "",
      "utmv" -> "",
      "utmx" -> "",
      "utmz" -> "",
      "cv" -> "clj-0.6.0-tom-0.0.4",
      "nuid" -> "-"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.source, Shared.context)
    val actual = CallrailAdapter.toRawEvents[Eval](payload, SpecHelpers.client).value

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.callrail/call_complete/jsonschema/1-0-2",
              |"data":{
                |"duration":247,
                |"utm_source":null,
                |"utmv":null,
                |"ip":"86.178.163.7",
                |"utmx":null,
                |"ga":null,
                |"destinationnum":"2012032051",
                |"datetime":"2014-10-09T16:23:45.000Z",
                |"kissmetrics_id":null,
                |"landingpage":"http://acme.com/",
                |"callerzip":"92307",
                |"gclid":null,
                |"callername":"SKYPE CALLER",
                |"utmb":null,
                |"id":"201235151",
                |"callernum":"+12612230240",
                |"utm_content":null,
                |"trackingnum":"+12012311668",
                |"referrermedium":"Direct",
                |"utm_campaign":null,
                |"keywords":null,
                |"transcription":null,
                |"utmz":null,
                |"utma":null,
                |"referrer":"direct",
                |"callerstate":"CA",
                |"recording":"http://app.callrail.com/calls/201235151/recording/9f59ad59ba1cfa264312",
                |"first_call":true,
                |"utmc":null,
                |"callercountry":"US",
                |"utm_medium":null,
                |"callercity":"BAKERSFIELD",
                |"utm_term":null,
                |"answered":true,
                |"callsource":"keyword"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    actual must beValid(
      NonEmptyList.one(
        RawEvent(
          Shared.api,
          Expected.static ++ Map("ue_pr" -> expectedJson, "nuid" -> "-"),
          None,
          Shared.source,
          Shared.context
        )
      )
    )
  }

  def e2 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.source, Shared.context)
    val actual = CallrailAdapter.toRawEvents[Eval](payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("querystring", None, "empty querystring")
      )
    )
  }
}
