/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive

// Java
import java.util.{ArrayList => JArrayList}

// Scala
import scala.collection.JavaConversions

// Specs2
import org.specs2.mutable.Specification

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

class MarketingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // TODO: generalise this so every test can use it (and add in the other fields)
  case class SnowPlowEvent(
    mkt_medium: String,
    mkt_source: String,
    mkt_term: String,
    mkt_content: String,
    mkt_campaign: String
  ) 

  val testData = List("2012-08-05	19:36:15	LHR5	3345	179.74.195.71	GET	d3gs014xn8p70.cloudfront.net /ice.png	200	https://www.psychicbazaar.com/shop/checkout/	Mozilla/5.0%20(compatible;%20MSIE%209.0;%20Windows%20NT%206.1;%20WOW64;%20Trident/5.0)	page=Psychic%2520Bazaar%2520Checkout&tid=993364&uid=08793baa435a9c6e&vid=1&lang=en-gb&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utm_source%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dangel%252520cards%2526utm_content%253D27719551768%2526gclid%253DCPL94Zqd0bECFQRTfAodDCAAbQ&f_java=1&res=1366x768&cookie=1&url=https%253A%252F%252Fwww.psychicbazaar.com%252Fshop%252Fcheckout%252F" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO"),
                      "2012-08-05	19:36:15	LHR5	3345	179.74.195.71	GET	d3gs014xn8p70.cloudfront.net /ice.png	200	https://www.psychicbazaar.com/shop/checkout/	Mozilla/5.0%20(compatible;%20MSIE%209.0;%20Windows%20NT%206.1;%20WOW64;%20Trident/5.0)	page=Psychic%2520Bazaar%2520Checkout&tid=993364&uid=08793baa435a9c6e&vid=1&lang=en-gb&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utm_source%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dangel%252520cards%2526utm_content%253D27719551768%2526gclid%253DCPL94Zqd0bECFQRTfAodDCAAbQ&f_java=1&res=1366x768&cookie=1&url=https%253A%252F%252Fwww.psychicbazaar.com%252Fshop%252Fcheckout%252F" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO"),
                      "2012-08-05	19:36:15	LHR5	3345	179.74.195.71	GET	d3gs014xn8p70.cloudfront.net /ice.png	200	https://www.psychicbazaar.com/shop/checkout/	Mozilla/5.0%20(compatible;%20MSIE%209.0;%20Windows%20NT%206.1;%20WOW64;%20Trident/5.0)	page=Psychic%2520Bazaar%2520Checkout&tid=993364&uid=08793baa435a9c6e&vid=1&lang=en-gb&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F11-oracles%252Fgenre-oracles%252Fangels%252Ftype%252Fall%252Fview%252Fgrid%253Fn%253D48%2526utm_source%253DGoogleSearch%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-oracle-decks--angel-cards-text%2526utm_term%253Dangel%252520cards%2526utm_content%253D27719551768%2526gclid%253DCPL94Zqd0bECFQRTfAodDCAAbQ&f_java=1&res=1366x768&cookie=1&url=https%253A%252F%252Fwww.psychicbazaar.com%252Fshop%252Fcheckout%252F" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO")
                     )

  "A SnowPlow event with marketing fields should have this marketing data extracted" >> {
    testData foreach { case (row, expected) =>

       val actual = SnowPlowEventDeserializer.deserializeLine(row, DEBUG).asInstanceOf[SnowPlowEventStruct]

      "The SnowPlow row \"%s\"".format(row) should {
        "have mkt_medium (Medium) = %s".format(expected.mkt_medium) in {
          actual.mkt_medium must_== expected.mkt_medium
        }
      }
    }
  }
}
