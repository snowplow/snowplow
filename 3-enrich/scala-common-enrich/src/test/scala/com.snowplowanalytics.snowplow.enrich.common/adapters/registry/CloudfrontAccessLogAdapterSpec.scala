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
  "toRawEvents should return a NEL containing one RawEvent if the line contains 23 fields"                        ! e5^
  "toRawEvents should return a Validation Failure if the line is the wrong length"                                ! e6^
  "toRawEvents should return a Validation Failure if the line contains an unparseable field"                      ! e7^
                                                                                                                  end

  implicit val resolver = SpecHelpers.IgluResolver
  val loader = new TsvLoader("com.amazon.aws.cloudfront/wd_access_log")

  val doubleEncodedUa = "Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36"
  val singleEncodedUa = "Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_9_2)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/34.0.1847.131%20Safari/537.36"
  val unEncodedUa =     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36"

  val doubleEncodedQs = "a=b%2520c"
  val singleEncodedQs = "a=b%20c"

  val url = "http://snowplowanalytics.com/analytics/index.html"

  object Shared {
    val api = CollectorApi("com.amazon.aws.cloudfront", "wd_access_log")
    val source = CollectorSource("tsv", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-10-07T23:35:30.000Z").some,
      "255.255.255.255".some,
      singleEncodedUa.some,
      None,
      Nil,
      None)
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.amazon.aws.cloudfront/wd_access_log",
      "e"  -> "ue",
      "url" -> url
      )
    val static = staticNoPlatform ++ Map(
      "p"  -> "srv"
    )
  }

  def e1 = {

    val input = s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-0",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e2 = {

    val input = s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-1",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e3 = {

    val input = s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-2",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

def e4 = {

    val input = s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-3",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

  def e5 = {

    val input = s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001\tr\ts\tt\tu"

    val payload = loader.toCollectorPayload(input)

    val actual = payload.map(_.map(CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(_)))

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-4",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001,
                |"xForwardedFor":"r",
                |"sslProtocol":"s",
                |"sslCipher":"t",
                |"xEdgeResponseResultType":"u"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(Some(Success(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.source, Shared.context)))))
  }

  def e6 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, "2013-10-07\t23:35:30\tc\t\t".some, Shared.source, Shared.context)
    val actual = CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Access log TSV line contained 5 fields, expected 12, 15, 18, 19, or 23"))
  }

  def e7 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, "a\tb\tc\td\te\tf\tg\th\ti\t$url\tk\t$doubleEncodedQs".some, Shared.source, Shared.context)
    val actual = CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Unexpected exception converting Cloudfront web distribution access log date [a] and time [b] to timestamp: [Invalid format: \"aTb+00:00\"]", "Field [scBytes]: cannot convert [d] to Int"))
  }
}
