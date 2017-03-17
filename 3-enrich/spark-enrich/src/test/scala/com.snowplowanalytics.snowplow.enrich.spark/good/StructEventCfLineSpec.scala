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

object StructEventCfLineSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2012-05-27  11:35:53  DFW3  3343  255.255.255.255 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/internal/_session/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=se&se_ca=ecomm&se_ac=add-to-basket&se_la=%CE%A7%CE%B1%CF%81%CE%B9%CF%84%CE%AF%CE%BD%CE%B7&se_pr=1&ip=70.46.123.145&se_va=35708.23&dtm=1364230969450&tid=598951&evn=com.snowplowanalytics&vp=2560x934&ds=2543x1420&vid=43&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2F&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Foracles%2F119-psycards-book-and-deck-starter-pack.html%3Fview%3Dprint%23detail"
  )
  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2012-05-27 11:35:53.000",
    "2013-03-25 17:02:49.450",
    "struct",
    null, // We can't predict the event_id
    "598951",
    null, // No tracker namespace
    "js-0.11.1",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "70.46.x.x", // Anonymization and geo both work using &ip=
    "2876815413",
    "9795bd0203804cd1",
    "43",
    null, // No network_userid set
    "US", // Anonymization and geo both work using &ip=
    "FL",
    "Delray Beach",
    null,
    "26.461502",
    "-80.0728",
    "Florida",
    null, // No additional MaxMind databases used
    "DSLAM WAN Allocation", // Using the MaxMind organization lookup service
    null,
    null,
    "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail",
    null, // No page title for events
    "http://www.psychicbazaar.com/",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/oracles/119-psycards-book-and-deck-starter-pack.html",
    "view=print",
    "detail",
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
    "ecomm",         // Structured event fields are set
    "add-to-basket", //
    "Χαριτίνη",      // Check Unicode handling
    "1",             //
    "35708.23",      //
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
 * Check that all tuples in a custom structured event
 * (CloudFront format) are successfully extracted.
 */
class StructEventCfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "struct-event-cf-lines"
  sequential
  "A job which processes a CloudFront file containing 1 valid custom structured event" should {
    runEnrichJob(StructEventCfLineSpec.lines, "cloudfront", "2", true, List("geo", "organization"))

    "correctly output 1 custom structured event" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- StructEventCfLineSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(StructEventCfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
