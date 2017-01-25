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

object PageViewCfLineSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2012-05-24  00:06:42  LHR5  3402  70.46.123.145  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/crystals/335-howlite-tumble-stone.html?view=print#detail Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  &e=pv&eid=550e8400-e29b-41d4-a716-446655440000&page=Psychic%20Bazaar%09Shop&dtm=1364219529188&tid=637309&vp=2560x935&ds=2543x1273&vid=41&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fq%253Dgateway%252Boracle%252Bcards%252Bdenise%252Blinn%2526hl%253Den%2526client%253Dsafari&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Fcrystals%2F335-howlite-tumble-stone.html%3Fview%3Dprint%23detail&cv=clj-0.5.0-tom-0.0.4"
  )
  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2012-05-24 00:06:42.000",
    "2013-03-25 13:52:09.188",
    "page_view",
    "550e8400-e29b-41d4-a716-446655440000", // event_id is present in the querystring
    "637309",
    null, // No tracker namespace
    "js-0.11.1",
    "clj-0.5.0-tom-0.0.4",
    etlVersion,
    null, // No user_id set
    "70.46.123.x",
    "2876815413",
    "9795bd0203804cd1",
    "41",
    null, // No network_userid set
    "US", // US geolocation
    "FL",
    "Delray Beach",
    null,
    "26.461502",
    "-80.0728",
    "Florida",
    "FDN Communications", // Using the ISP lookup service
    null,
    null,
    null,
    "http://www.psychicbazaar.com/crystals/335-howlite-tumble-stone.html?view=print#detail",
    "Psychic Bazaar    Shop",
    "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/crystals/335-howlite-tumble-stone.html",
    "view=print",
    "detail",
    "http",
    "www.google.com",
    "80",
    "/search",
    "q=gateway+oracle+cards+denise+linn&hl=en&client=safari",
    null,
    "search", // Search referer
    "Google",
    "gateway oracle cards denise linn",
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
    null, // Unstructured event field empty
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
    "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3",
    "Mobile Safari",
    "Safari",
    "5.1",
    "Browser (mobile)",
    "WEBKIT",
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
    "935",
    "iOS 5 (iPhone)",
    "iOS",
    "Apple Inc.",
    "Europe/London",
    "Mobile",
    "1",
    "2560",
    "1440",
    "UTF-8",
    "2543",
    "1273"
  )
}

/**
 * Check that all tuples in a raw page view event (CloudFront format) are successfully extracted.
 */
class PageViewCfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "page-view-cf-lines"
  sequential
  "A job which processes a CloudFront file containing 1 valid page view event" should {
    // Anonymize 1 IP address quartet
    runEnrichJob(PageViewCfLineSpec.lines, "cloudfront", "1", true, List("geo", "isp"))

    "correctly output 1 page view" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- PageViewCfLineSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(PageViewCfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
