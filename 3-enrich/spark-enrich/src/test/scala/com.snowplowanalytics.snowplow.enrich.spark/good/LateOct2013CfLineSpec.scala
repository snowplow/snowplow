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

object LateOct2013CfLineSpec {
  import EnrichJobSpec._
  // 21 Oct 2013: Amazon added three fields onto the end of the CloudFront access log format
  val lines = Lines(
    "2013-10-22 00:41:30    LHR3    828 255.255.255.255   GET d10wr4jwvp55f9.cloudfront.net   /i  200 http://www.psychicbazaar.com/tarot-cards/304-russian-tarot-of-st-petersburg-set.html    Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_7_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/29.0.1547.76%2520Safari/537.36 e=pv&page=Russian%2520Tarot%2520of%2520St%2520Petersburg%2520Deck%2520-%2520Psychic%2520Bazaar&co=%257B%2522data%2522%253A%255B%257B%2522schema%2522%253A%2522iglu%253Acom.snowplowanalytics.snowplow%252Furi_redirect%252Fjsonschema%252F1-0-0%2522%252C%2522data%2522%253A%257B%2522uri%2522%253A%2522http%253A%252F%252Fsnowplowanalytics.com%252F%2522%257D%257D%255D%252C%2522schema%2522%253A%2522iglu%253Acom.snowplowanalytics.snowplow%252Fcontexts%252Fjsonschema%252F1-0-0%2522%257D&dtm=1382402513725&tid=888087&vp=1364x694&ds=1364x1570&vid=1&duid=be5a361443c84ba7&p=web&tv=js-0.12.0&fp=3596254824&aid=pbzsite&lang=zh-TW&cs=UTF-8&tz=Europe%252FLondon&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%253Futm_source%253DGoogleSearch%2526utm_term%253Dtarot%252520decks%2526utm_content%253D42017425288%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-tarot--TAROT-CARDS-GENERAL%2526gclid%253DCJ7G8MSaqboCFfHKtAodPS0AIw&f_pdf=1&f_qt=1&f_realp=1&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1440x900&cd=24&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F304-russian-tarot-of-st-petersburg-set.html    -   Hit wYSiKLXqmO2meYhvy7UrHoV8fFGt6jOYGY5rV2zYg05j1Qkirt83GA==    d10wr4jwvp55f9.cloudfront.net   http    1119"
  )
  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2013-10-22 00:41:30.000",
    "2013-10-22 00:41:53.725",
    "page_view",
    null, // We can't predict the event_id
    "888087",
    null, // No tracker namespace
    "js-0.12.0",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "255.255.255.255",
    "3596254824",
    "be5a361443c84ba7",
    "1",
    null, // No network_userid set
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    "http://www.psychicbazaar.com/tarot-cards/304-russian-tarot-of-st-petersburg-set.html",
    "Russian Tarot of St Petersburg Deck - Psychic Bazaar",
    "http://www.psychicbazaar.com/2-tarot-cards?utm_source=GoogleSearch&utm_term=tarot%20decks&utm_content=42017425288&utm_medium=cpc&utm_campaign=uk-tarot--TAROT-CARDS-GENERAL&gclid=CJ7G8MSaqboCFfHKtAodPS0AIw",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/tarot-cards/304-russian-tarot-of-st-petersburg-set.html",
    null,
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards",
    "utm_source=GoogleSearch&utm_term=tarot%20decks&utm_content=42017425288&utm_medium=cpc&utm_campaign=uk-tarot--TAROT-CARDS-GENERAL&gclid=CJ7G8MSaqboCFfHKtAodPS0AIw",
    null,
    "internal", // Search referer
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    """{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"http://snowplowanalytics.com/"}}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""",
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
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.76 Safari/537.36",
    "Chrome 29",
    "Chrome",
    "29.0.1547.76",
    "Browser",
    "WEBKIT",
    "zh-TW",
    "1",
    "1",
    "1",
    "0",
    "1",
    "1",
    "0",
    "0",
    "1",
    "1",
    "24",
    "1364",
    "694",
    "Mac OS X",
    "Mac OS X",
    "Apple Inc.",
    "Europe/London",
    "Computer",
    "0",
    "1440",
    "900",
    "UTF-8",
    "1364",
    "1570"
  )
}

/**
 * Check that all tuples in a page view in the CloudFront format changed in August 2013
 * are successfully extracted.
 * For details: https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class LateOct2013CfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "late-oct-2013-cf-lines"
  sequential
  "A job which processes a CloudFront file containing 1 valid page view" should {
    runEnrichJob(LateOct2013CfLineSpec.lines, "cloudfront", "1", false, List("geo"))

    "correctly output 1 page ping" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- LateOct2013CfLineSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(LateOct2013CfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
