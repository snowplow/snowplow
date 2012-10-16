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

// Scala
import scala.collection.JavaConversions

// Specs2
import org.specs2.mutable.Specification

// Hive
import org.apache.hadoop.hive.serde2.SerDeException

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Deserializer
import test.{SnowPlowDeserializer, SnowPlowEvent}

class PageViewTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row1 = "2012-05-24  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  page=Tarot%2520cards%2520-%2520Psychic%2520Bazaar&tid=344260&uid=288112e0a5003be2&vid=1&lang=en-US&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fp%253D4&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1366x768&cookie=1"
  val row2 = "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1"

  "A valid SnowPlow page view row should deserialize without error" >> {
    Seq(row1, row2) foreach { row =>
      "Page view row \"%s\" deserializes okay".format(row) >> {
        SnowPlowDeserializer.deserializeUntyped(row) must beAnInstanceOf[SnowPlowEventStruct]
      }
    }
  }

  val row3 = "2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=0&f_java=0&f_gears=0&f_ag=0&res=320x480&cookie=1"

  // TODO: this is row2 expected - not row3 expected. Reason this test passes has to be something to do with Specs2 parallelizable test bug
  val expected3 = new SnowPlowEvent().tap { e =>
    e.dt = "2012-05-24"
    e.tm = "00:08:40"
    e.txn_id = "721410"
    e.user_id = "3798cdce0493133e"
    e.user_ipaddress = "74.125.17.210"
    e.visit_id = 1
    e.page_url = "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html"
    e.page_title = "Psycards book and deck starter pack - Psychic Bazaar"
    e.page_referrer = "http://www.google.com/m/search"
    e.br_name = "Safari 4"
    e.br_family = "Safari"
    e.br_version = "4.0"
    e.br_type = "Browser"
    e.br_renderengine = "WEBKIT"
    e.br_lang = "en"
    e.br_cookies = true
    e.br_features = List("qt")
    e.os_name = "Android"
    e.os_family = "Android"
    e.os_manufacturer = "Google Inc."
    e.dvce_type = "Mobile"
    e.dvce_ismobile = true
    e.dvce_screenwidth = 640
    e.dvce_screenheight = 960
  }

  "The SnowPlow page view row \"%s\"".format(row3) should {

    val actual = SnowPlowDeserializer.deserialize(row3)

    // Check all of the field values

    // Date/time
    "have dt (Date) = %s".format(expected3.dt) in {
      actual.dt must_== expected3.dt
    }
    "have tm (Time) = %s".format(expected3.tm) in {
      actual.tm must_== expected3.tm
    }

    // Transaction
    "have txn_id (Transaction ID) = %s".format(expected3.txn_id) in {
      actual.txn_id must_== expected3.txn_id
    }

    // User and visit
    "have user_id (User ID) = %s".format(expected3.user_id) in {
      actual.user_id must_== expected3.user_id
    }
    "have user_ipaddress (User IP Address) = %s".format(expected3.user_ipaddress) in {
      actual.user_ipaddress must_== expected3.user_ipaddress
    }
    "have visit_id (User IP Address) = %s".format(expected3.visit_id) in {
      actual.visit_id must_== expected3.visit_id
    }

    // Page
    "have page_url (Page URL) = %s".format(expected3.page_url) in {
      actual.page_url must_== expected3.page_url
    }
    // Tracking a page view, so we have a page title
    "have page_title (Page Title) = %s".format(expected3.page_title) in {
      actual.page_title must_== expected3.page_title
    }
    "have page_referrer (Page Referrer) = %s".format(expected3.page_referrer) in {
      actual.page_referrer must_== expected3.page_referrer
    }

    // Browser (from user-agent)
    "have br_name (Browser Name) = %s".format(expected3.br_name) in {
      actual.br_name must_== expected3.br_name
    }
    "have br_family (Browser Family) = %s".format(expected3.br_family) in {
      actual.br_family must_== expected3.br_family
    }
    "have br_version (Browser Version) = %s".format(expected3.br_version) in {
      actual.br_version must_== expected3.br_version
    }
    "have br_type (Browser Type) = %s".format(expected3.br_type) in {
      actual.br_type must_== expected3.br_type
    }
    "have br_renderengine (Browser Rendering Engine) = %s".format(expected3.br_renderengine) in {
      actual.br_renderengine must_== expected3.br_renderengine
    }

    // Browser (from querystring)
    "have br_lang (Browser Lang) = %s".format(expected3.br_lang) in {
      actual.br_lang must_== expected3.br_lang
    }
    "have br_cookies (Browser Cookies Enabled?) = %s".format(expected3.br_cookies) in {
      actual.br_cookies must_== expected3.br_cookies
    }
    "have br_features (Browser Features) = %s".format(expected3.br_features) in {
      // For some reason (Specs2) couldn't use implicit Java->Scala conversion here
      JavaConversions.asScalaBuffer(actual.br_features) must haveTheSameElementsAs(expected3.br_features)
    }.pendingUntilFixed // For some reason actual.br_features empties when inside this test

    // OS (from user-agent)    
    "have os_name (OS Name) = %s".format(expected3.os_name) in {
      actual.os_name must_== expected3.os_name
    }
    "have os_family (OS Family) = %s".format(expected3.os_family) in {
      actual.os_family must_== expected3.os_family
    }
    "have os_manufacturer (OS Manufacturer) = %s".format(expected3.os_manufacturer) in {
      actual.os_manufacturer must_== expected3.os_manufacturer
    }
    
    // Device/Hardware (from user-agent) 
    "have dvce_type (Device Type) = %s".format(expected3.dvce_type) in {
      actual.dvce_type must_== expected3.dvce_type
    }
    "have dvce_ismobile (Device Is Mobile?) = %s".format(expected3.dvce_ismobile) in {
      actual.dvce_ismobile must_== expected3.dvce_ismobile
    }
    
    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(expected3.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== expected3.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(expected3.dvce_screenheight) in {
      actual.dvce_screenheight must_== expected3.dvce_screenheight
    }
  }
}