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
  TsvLoader,
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

class CloudfrontAccessLogAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the CloudfrontAccessLogAdapter functionality"                                             ^
                                                                                                                 p^
  "toRawEvents should return a NEL containing one RawEvent if the line contains 12 fields"                        ! e1^
  "toRawEvents should return a NEL containing one RawEvent if the line contains 15 fields"                        ! e2^
  "toRawEvents should return a NEL containing one RawEvent if the line contains 18 fields"                        ! e3^
  "toRawEvents should return a NEL containing one RawEvent if the line contains 19 fields"                        ! e4^
  "toRawEvents should return a Validation Failure if the line is the wrong length"                                ! e5^
                                                                                                                  end

  implicit val resolver = SpecHelpers.IgluResolver
  val loader = new TsvLoader("com.amazon.aws.cloudfront/wd_access_log")

  object Shared {
    val api = CollectorApi("com.amazon.aws.cloudfront", "wd_access_log")
    val source = CollectorSource("tsv", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-10-07T23:35:30.000Z").some, "255.255.255.255".some, None, None, Nil, None)
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.amazon.aws.cloudfront/wd_access_log",
      "e"  -> "ue"
      )
    val static = staticNoPlatform ++ Map(
      "p"  -> "srv"
    )
  }

  def e1 = {

    val input = "2013-10-07\t23:35:30\tc\td\t255.255.255.255\tf\tg\th\ti\tj\tk\tl"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-0",
              |"data":{
                |"dateTime":"2013-10-07T23%3A35%3A30Z",
                |"xEdgeLocation":"c",
                |"scBytes":"d",
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"j",
                |"csUserAgent":"k",
                |"csUriQuery":"l"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e2 = {

    val input = "2013-10-07\t23:35:30\tc\td\t255.255.255.255\tf\tg\th\ti\tj\tk\tl\tm\tn\to"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-1",
              |"data":{
                |"dateTime":"2013-10-07T23%3A35%3A30Z",
                |"xEdgeLocation":"c",
                |"scBytes":"d",
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"j",
                |"csUserAgent":"k",
                |"csUriQuery":"l",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e3 = {

    val input = "2013-10-07\t23:35:30\tc\td\t255.255.255.255\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-2",
              |"data":{
                |"dateTime":"2013-10-07T23%3A35%3A30Z",
                |"xEdgeLocation":"c",
                |"scBytes":"d",
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"j",
                |"csUserAgent":"k",
                |"csUriQuery":"l",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":"r"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e4 = {

    val input = "2013-10-07\t23:35:30\tc\td\t255.255.255.255\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr\ts"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-3",
              |"data":{
                |"dateTime":"2013-10-07T23%3A35%3A30Z",
                |"xEdgeLocation":"c",
                |"scBytes":"d",
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"j",
                |"csUserAgent":"k",
                |"csUriQuery":"l",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":"r",
                |"timeTaken":"s"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

  def e5 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, "2013-10-07\t23:35:30\tc\t\t".some, Shared.source, Shared.context)
    val actual = CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Access log TSV line contained 5 fields, expected 12, 15, 18, or 19"))
  }
}
