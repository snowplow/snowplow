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

object CurrencyConversionTransactionSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2012-05-27  11:35:53  DFW3  3343  70.46.123.145 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 - Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=tr&tr_id=order-123&tr_af=psychicbazaar&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&dtm=1364177017342&tr_cu=USD&cx=ewoicGFnZSI6eyJjYXRlZ29yeSI6InByb2R1Y3QiLCJza3UiOjM4Mn0sICJjb3VudHMiOiBbMS4wLCAyLjAsIDMuMCwgNC4wXQp9&tid=028288&duid=a279872d76480afb&vid=1&aid=CFe23a&lang=en-GB&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Foracles%2F119-psycards-book-and-deck-starter-pack.html%3Fview%3Dprint%23detail&cv=clj-0.3.0-tom-0.0.2"
  )

  val expected = List(
    "CFe23a",
    null, // Not set (legacy input line)
    etlTimestamp,
    "2012-05-27 11:35:53.000",
    "2013-03-25 02:03:37.342",
    "transaction",
    null, // We can't predict the event_id
    "028288",
    null, // No tracker namespace
    null, // Not set (legacy input line)
    "clj-0.3.0-tom-0.0.2",
    etlVersion,
    null, // No user_id set
    "x.x.x.x",
    null, // Not set (legacy input line)
    "a279872d76480afb",
    "1",
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
    null,
    "Cable/DSL", // Using the MaxMind netspeed lookup service
    "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail",
    null, // No page title for transactions
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/oracles/119-psycards-book-and-deck-starter-pack.html",
    "view=print",
    "detail",
    null, // No referrer URL components
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // No referrer details
    null, //
    null, //
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    """{"page":{"category":"product","sku":382},"counts":[1.0,2.0,3.0,4.0]}""",
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event field empty
    "order-123",     // Transaction fields are set
    "psychicbazaar", //
    "8000",          //
    "200",           //
    "50",            //
    "London",        //
    "England",       //
    "UK",            //
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
    "0",
    "1",
    "1",
    "0",
    "1",
    "0",
    "1",
    "0",
    "0",
    "1",
    null, // Not set (legacy input lines)
    null, //
    null, //
    "Windows 7",
    "Windows",
    "Microsoft Corporation",
    null, // Not set (legacy input line)
    "Computer",
    "0",
    "1920",
    "1080",
    null, // Not set (legacy input lines)
    null,
    null,
    "USD",     // tr_currency
    "6384.46", // tr_total_base
    "159.61",  // tr_tax_base
    "39.90",   // tr_shipping_base
    null,      // ti_currency
    null,      // ti_price_base
    "EUR",     // base_currency
    "America/New_York",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )
}

/** Check that the currency conversion enrichment works for transaction events. */
class CurrencyConversionTransactionSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "currency-conversion-transaction"
  sequential
  // Only run currency enrichment tests if the credentials exist in an environment variable
  if (sys.env.get("OER_KEY").isDefined) {
    "A job which processes a CloudFront file containing 1 valid transaction" should {
      runEnrichJob(CurrencyConversionTransactionSpec.lines, "cloudfront", "4", true,
        List("geo", "netspeed"), true)

      "correctly output 1 transaction" in {
        val Some(goods) = readPartFile(dirs.output)
        goods.size must_== 1
        val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
        for (idx <- CurrencyConversionTransactionSpec.expected.indices) {
          actual(idx) must beFieldEqualTo(CurrencyConversionTransactionSpec.expected(idx), idx)
        }
      }

      "not write any bad rows" in {
        dirs.badRows must beEmptyDir
      }
    }
  } else {
    println("WARNING: Skipping CurrencyConversionTransactionSpec as no OER_KEY environment variable was found for authentication")
  }
}
