/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark
package good

import org.specs2.mutable.Specification

object UnstructEventBase64CfLineSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2012-05-27  11:35:53  DFW3  3343  70.46.123.145 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=ue&ue_px=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy13ZWJzaXRlL3NpZ251cF9mb3JtX3N1Ym1pdHRlZC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYW1lIjoizqfOsc-BzrnPhM6vzr3OtyBORVcgVW5pY29kZSB0ZXN0IiwiZW1haWwiOiJhbGV4K3Rlc3RAc25vd3Bsb3dhbmFseXRpY3MuY29tIiwiY29tcGFueSI6IlNQIiwiZXZlbnRzUGVyTW9udGgiOiI8IDEgbWlsbGlvbiIsInNlcnZpY2VUeXBlIjoidW5zdXJlIn19fQ&dtm=1364230969450&evn=com.acme&tid=598951&vp=2560x934&ds=2543x1420&vid=43&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2F&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2F2-tarot-cards"
  )
  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2012-05-27 11:35:53.000",
    "2013-03-25 17:02:49.450",
    "unstruct",
    null, // We can't predict the event_id
    "598951",
    null, // No tracker namespace
    "js-0.11.1",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "70.46.123.145",
    "2876815413",
    "9795bd0203804cd1",
    "43",
    null, // No network_userid set
    "US", // US geolocation
    "FL",
    "Delray Beach",
    null,
    "26.461502",
    "-80.0728",
    "Florida",
    null,
    null,
    "nuvox.net",  // Using the MaxMind domain lookup service
    null,
    "http://www.psychicbazaar.com/2-tarot-cards",
    null, // No page title for events
    "http://www.psychicbazaar.com/",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards",
    null,
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/",
    null,
    null,
    "internal", // Internal referer
    null,
    null,
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0","data":{"name":"Χαριτίνη NEW Unicode test","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}}}""", // Unstructured event field set
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields are empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0",
    "Firefox 12",
    "Firefox",
    "12.0",
    "Browser",
    "GECKO",
    "en-GB",
    "1",
    "1",
    "1",
    "0",
    "0",
    "0",
    "0",
    "0",
    "1",
    "1",
    "32",
    "2560",
    "934",
    "Windows 7",
    "Windows",
    "Microsoft Corporation",
    "Europe/London",
    "Computer",
    "0",
    "2560",
    "1440",
    "UTF-8",
    "2543",
    "1420"
  )
}

/**
 * Check that all tuples in a custom unstructured event
 * (CloudFront format) are successfully extracted.
 */
class UnstructEventBase64CfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "unstruct-event-base64-cf-lines"
  sequential
  "A job which processes a CloudFront file containing 1 valid custom unstructured event" should {
    runEnrichJob(UnstructEventBase64CfLineSpec.lines, "cloudfront", "1", false,
      List("geo", "domain"))

    "correctly output 1 custom unstructured event" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- UnstructEventBase64CfLineSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(UnstructEventBase64CfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
