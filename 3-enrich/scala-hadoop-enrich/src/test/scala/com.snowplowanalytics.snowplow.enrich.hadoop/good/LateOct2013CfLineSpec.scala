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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package good

// Scala
import scala.collection.mutable.Buffer

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

/**
 * Holds the input and expected data
 * for the test.
 */
object LateOct2013CfLineSpec {

  // 21 Oct 2013: Amazon added three fields onto the end of the CloudFront access log format
  val lines = Lines(
    "2013-10-22 00:41:30    LHR3    828 255.255.255.255   GET d10wr4jwvp55f9.cloudfront.net   /i  200 http://www.psychicbazaar.com/tarot-cards/304-russian-tarot-of-st-petersburg-set.html    Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_7_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/29.0.1547.76%2520Safari/537.36 e=pv&page=Russian%2520Tarot%2520of%2520St%2520Petersburg%2520Deck%2520-%2520Psychic%2520Bazaar&co=%257B%2522page%2522%253A%257B%2522category%2522%253A%2522product%2522%252C%2522sku%2522%253A382%257D%257D&dtm=1382402513725&tid=888087&vp=1364x694&ds=1364x1570&vid=1&duid=be5a361443c84ba7&p=web&tv=js-0.12.0&fp=3596254824&aid=pbzsite&lang=zh-TW&cs=UTF-8&tz=Europe%252FLondon&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%253Futm_source%253DGoogleSearch%2526utm_term%253Dtarot%252520decks%2526utm_content%253D42017425288%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-tarot--TAROT-CARDS-GENERAL%2526gclid%253DCJ7G8MSaqboCFfHKtAodPS0AIw&f_pdf=1&f_qt=1&f_realp=1&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1440x900&cd=24&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F304-russian-tarot-of-st-petersburg-set.html    -   Hit wYSiKLXqmO2meYhvy7UrHoV8fFGt6jOYGY5rV2zYg05j1Qkirt83GA==    d10wr4jwvp55f9.cloudfront.net   http    1119"
    )

  val expected = List(
    "pbzsite",
    "web",
    "2013-10-22 00:41:30.000",
    "2013-10-22 00:41:53.725",
    "page_view",
    null, // No event vendor set
    null, // We can't predict the event_id
    "888087",
    null, // No tracker namespace
    "js-0.12.0",
    "cloudfront",
    EtlVersion,
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
    """{"page":{"category":"product","sku":382}}""", // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event fields empty
    null, //
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
    "Mac OS",
    "Mac OS",
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
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page view in the
 * CloudFront format changed in August 2013
 * are successfully extracted.
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class LateOct2013CfLineSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid page view" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), LateOct2013CfLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- LateOct2013CfLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(LateOct2013CfLineSpec.expected(idx), withIndex = idx)
          }
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}