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
package adapters
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class AdxtrackingAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the AdxtrackingAdapter functionality"                                                     ^
                                                                                                                            p^
  "toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is minimally populated"             ! e1^
  "toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is maximally populated"             ! e2^
  "toRawEvents should return a NEL containing one RawEvent if the Clojure-Tomcat querystring is populated"                   ! e3^
  "toRawEvents should return a NEL containing one RawEvent with a non-app platform if specified"                             ! e4^
  "toRawEvents should return a Validation Failure if there are no parameters on the CloudFront querystring"                  ! e5^
                                                                                                                             end

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.adxtracking", "v1")
    val cfSource = CollectorSource("cloudfront", "UTF-8", None)
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.adxtracking-v1",
      "e"  -> "ue"
      )
    val static = staticNoPlatform ++ Map(
      "p"  -> "app"
    )
  }

  def e1 = {
    val params = toNameValuePairs(
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "download",
      "publisher_name" -> "Organic",
      "sub_campaign"   -> "",
      "tracking_id"    -> "",
      "sub_adgroup"    -> ""
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = AdxtrackingAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.adxtracking/user_interaction/jsonschema/1-0-0",
              |"data":{
                |"name":"download",
                |"sub_adgroup":null,
                |"sub_campaign":null,
                |"tracking_id":null,
                |"publisher_name":"Organic",
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.cfSource, Shared.context)))
  }

  def e2 = {
    val params = toNameValuePairs(
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "install",
      "publisher_name" -> "Organic",
      "sub_campaign"   -> "newsfeed",
      "tracking_id"    -> "3353af9c-e298-2cf3-9f1c-b377a9c84dad",
      "sub_adgroup"    -> "UN-11-b",
      "aid"            -> "webhooks"
      )    
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = AdxtrackingAdapter.toRawEvents(payload)
    
    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.adxtracking/user_interaction/jsonschema/1-0-0",
                |"data":{
                  |"name":"install",
                  |"sub_adgroup":"UN-11-b",
                  |"sub_campaign":"newsfeed",
                  |"tracking_id":"3353af9c-e298-2cf3-9f1c-b377a9c84dad",
                  |"publisher_name":"Organic",
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]","")
      Map(
        "ue_pr" -> json,
        "aid"   -> "webhooks"
      )
    }

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cfSource, Shared.context)))
  }

  def e3 = {
    val params = toNameValuePairs(
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "retarget",
      "publisher_name" -> "Organic",
      "sub_campaign"   -> "newsfeed",
      "tracking_id"    -> "",
      "sub_adgroup"    -> "UN-11-b",
      "aid"            -> "my webhook project",
      "cv"             -> "clj-0.5.0-tom-0.0.4",
      "nuid"           -> ""
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cljSource, Shared.context)
    val actual = AdxtrackingAdapter.toRawEvents(payload)
    
    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.adxtracking/user_interaction/jsonschema/1-0-0",
                |"data":{
                  |"name":"retarget",
                  |"sub_adgroup":"UN-11-b",
                  |"sub_campaign":"newsfeed",
                  |"tracking_id":null,
                  |"publisher_name":"Organic",
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]","")
      Map(
        "ue_pr" -> json,
        "aid"   -> "my webhook project",
        "cv"    -> "clj-0.5.0-tom-0.0.4",
        "nuid"  -> ""
      )
    }

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cljSource, Shared.context)))
  }

  def e4 = {
    val params = toNameValuePairs(
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "download",
      "publisher_name" -> "Organic",
      "p"              -> "mob"
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = AdxtrackingAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.adxtracking/user_interaction/jsonschema/1-0-0",
              |"data":{
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac",
                |"name":"download",
                |"publisher_name":"Organic"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.staticNoPlatform ++ Map("p" -> "mob", "ue_pr" -> expectedJson), None, Shared.cfSource, Shared.context)))
  }

  def e5 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = AdxtrackingAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Querystring is empty: no AD-X Tracking event to process"))
  }
}
