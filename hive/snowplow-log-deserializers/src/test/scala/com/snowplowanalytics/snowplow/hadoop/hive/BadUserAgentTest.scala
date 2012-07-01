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

// Specs2
import org.specs2.mutable.Specification

class NoUserAgentTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // Template row to hold an empty
  val rowWithUseragent: (String => String) = "2012-05-28  21:12:03  IAD12 3402  71.191.251.183  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/tarot-cards/57-universal-rider-waite-pocket-tarot-deck.html  " + _ + "  page=Universal%2520Rider%2520Waite%2520pocket%2520Tarot%2520deck%2520-%2520Psychic%2520Bazaar&tid=491830&uid=80822abc1ad45c78&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.bing.com%252Fimages%252Fsearch%253Fq%253Drider%252Bwaite%252Btarot%252Bdeck%252Bcard%252Bimages%2526view%253Ddetail%2526id%253D9A4E92316CFA727D79FE6AD095D12C6B34DAB043%2526first%253D0%2526qpvt%253Drider%252Bwaite%252Btarot%252Bdeck%252Bcard%252Bimages%2526FORM%253DIDFRIR&f_java=1&res=1600x900&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F57-universal-rider-waite-pocket-tarot-deck.html"

  "An empty/bad useragent should be identified as unknown browser" >> {
    Seq("-", "SECRET%20BROWSER") foreach { ua =>
      "empty/bad useragent \"%s\" produces unknown browser".format(ua) >> {
        val actual = SnowPlowEventDeserializer.deserializeLine(rowWithUseragent(ua), DEBUG).asInstanceOf[SnowPlowEventStruct]
        actual.br_name must_== "Unknown"
      }
    }
  }

  "The user-agent \"NEW SPIDER USERAGENT\"" should {
    "be identified as a spider" in {
      val actual = SnowPlowEventDeserializer.deserializeLine(rowWithUseragent("NEW%20SPIDER%20USERAGENT"), DEBUG).asInstanceOf[SnowPlowEventStruct]
      actual.br_name must_== "Robot/Spider"
    }
  } 
}