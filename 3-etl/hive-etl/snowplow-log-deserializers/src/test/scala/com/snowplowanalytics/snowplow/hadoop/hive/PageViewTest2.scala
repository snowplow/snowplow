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

class PageViewTest2 extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row = "2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1"

  val expected = new SnowPlowEvent().tap { e =>
    e.dt = "2012-05-24"
    e.tm = "00:06:42"
    e.txn_id = "019539"
    e.user_id = "e7bccbb647296c98"
    e.user_ipaddress = "90.194.12.51"
    e.visit_id = 1
    e.page_url = "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html"
    e.page_title = "Psycards book and deck starter pack - Psychic Bazaar"
    e.page_referrer = "http://www.google.com/search?hl=en&q=the+psycard+story&oq=the+psycard+story&aq=f&aqi=&aql=&gs_l=mobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ&mvs=0"
    e.br_name = "Mobile Safari"
    e.br_family = "Safari"
    e.br_version = "5.1"
    e.br_type = "Browser (mobile)"
    e.br_renderengine = "WEBKIT"
    e.br_lang = "en-us"
    e.br_cookies = true
    e.br_cookies_bt = 1
    e.br_features = List("qt", "fla", "wma", "gears")
    e.br_features_pdf = 0
    e.br_features_flash = 1
    e.br_features_java = 0
    e.br_features_director = 0
    e.br_features_quicktime = 1
    e.br_features_realplayer = 0
    e.br_features_windowsmedia = 1
    e.br_features_gears = 1
    e.br_features_silverlight = 0
    e.os_name = "iOS 5 (iPhone)"
    e.os_family = "iOS"
    e.os_manufacturer = "Apple Inc."
    e.dvce_type = "Mobile"
    e.dvce_ismobile = true
    e.dvce_ismobile_bt = 1
    e.dvce_screenwidth = 320
    e.dvce_screenheight = 480
  }

  "The SnowPlow page view row \"%s\"".format(row) should {

    val actual = SnowPlowDeserializer.deserialize(row)

    // Check all of the field values

    // Date/time
    "have dt (Date) = %s".format(expected.dt) in {
      actual.dt must_== expected.dt
    }
    "have tm (Time) = %s".format(expected.tm) in {
      actual.tm must_== expected.tm
    }

    // Transaction
    "have txn_id (Transaction ID) = %s".format(expected.txn_id) in {
      actual.txn_id must_== expected.txn_id
    }

    // User and visit
    "have user_id (User ID) = %s".format(expected.user_id) in {
      actual.user_id must_== expected.user_id
    }
    "have user_ipaddress (User IP Address) = %s".format(expected.user_ipaddress) in {
      actual.user_ipaddress must_== expected.user_ipaddress
    }
    "have visit_id (User IP Address) = %s".format(expected.visit_id) in {
      actual.visit_id must_== expected.visit_id
    }

    // Page
    "have page_url (Page URL) = %s".format(expected.page_url) in {
      actual.page_url must_== expected.page_url
    }
    // Tracking a page view, so we have a page title
    "have page_title (Page Title) = %s".format(expected.page_title) in {
      actual.page_title must_== expected.page_title
    }
    "have page_referrer (Page Referrer) = %s".format(expected.page_referrer) in {
      actual.page_referrer must_== expected.page_referrer
    }

    // Browser (from user-agent)
    "have br_name (Browser Name) = %s".format(expected.br_name) in {
      actual.br_name must_== expected.br_name
    }
    "have br_family (Browser Family) = %s".format(expected.br_family) in {
      actual.br_family must_== expected.br_family
    }
    "have br_version (Browser Version) = %s".format(expected.br_version) in {
      actual.br_version must_== expected.br_version
    }
    "have br_type (Browser Type) = %s".format(expected.br_type) in {
      actual.br_type must_== expected.br_type
    }
    "have br_renderengine (Browser Rendering Engine) = %s".format(expected.br_renderengine) in {
      actual.br_renderengine must_== expected.br_renderengine
    }

    // Browser (from querystring)
    "have br_lang (Browser Lang) = %s".format(expected.br_lang) in {
      actual.br_lang must_== expected.br_lang
    }
    "have br_cookies (Browser Cookies Enabled?) = %s".format(expected.br_cookies) in {
      actual.br_cookies must_== expected.br_cookies
    }
    "have br_cookies_bt (Browser Cookies Enabled, Byte?) = %s".format(expected.br_cookies_bt) in {
      actual.br_cookies_bt must_== expected.br_cookies_bt
    }
    "have br_features (Browser Features) = %s".format(expected.br_features) in {
      // For some reason (Specs2) couldn't use implicit Java->Scala conversion here
      JavaConversions.asScalaBuffer(actual.br_features) must haveTheSameElementsAs(expected.br_features)
    }

    // Browser features
    "have br_features_pdf (Browser Feature PDF) = %s".format(expected.br_features_pdf) in {
      actual.br_features_pdf must_== expected.br_features_pdf
    }
    "have br_features_flash (Browser Feature Flash) = %s".format(expected.br_features_flash) in {
      actual.br_features_flash must_== expected.br_features_flash
    }
    "have br_features_java (Browser Feature Java) = %s".format(expected.br_features_java) in {
      actual.br_features_java must_== expected.br_features_java
    }
    "have br_features_director (Browser Feature Director) = %s".format(expected.br_features_director) in {
      actual.br_features_director must_== expected.br_features_director
    }
    "have br_features_quicktime (Browser Feature QuickTime) = %s".format(expected.br_features_quicktime) in {
      actual.br_features_quicktime must_== expected.br_features_quicktime
    }
    "have br_features_realplayer (Browser Feature RealPlayer) = %s".format(expected.br_features_realplayer) in {
      actual.br_features_realplayer must_== expected.br_features_realplayer
    }
    "have br_features_windowsmedia (Browser Feature Windows Media) = %s".format(expected.br_features_windowsmedia) in {
      actual.br_features_windowsmedia must_== expected.br_features_windowsmedia
    }
    "have br_features_gears (Browser Feature Google Gears) = %s".format(expected.br_features_gears) in {
      actual.br_features_gears must_== expected.br_features_gears
    }
    "have br_features_silverlight (Browser Feature Silverlight) = %s".format(expected.br_features_silverlight) in {
      actual.br_features_silverlight must_== expected.br_features_silverlight
    }

    // OS (from user-agent)
    "have os_name (OS Name) = %s".format(expected.os_name) in {
      actual.os_name must_== expected.os_name
    }.pendingUntilFixed // nl.bitwalker.useragentutils is not parsing this user agent correctly
    "have os_family (OS Family) = %s".format(expected.os_family) in {
      actual.os_family must_== expected.os_family
    }.pendingUntilFixed // nl.bitwalker.useragentutils is not parsing this user agent correctly
    "have os_manufacturer (OS Manufacturer) = %s".format(expected.os_manufacturer) in {
      actual.os_manufacturer must_== expected.os_manufacturer
    }

    // Device/Hardware (from user-agent)
    "have dvce_type (Device Type) = %s".format(expected.dvce_type) in {
      actual.dvce_type must_== expected.dvce_type
    }.pendingUntilFixed // nl.bitwalker.useragentutils is not parsing this user agent correctly
    "have dvce_ismobile (Device Is Mobile?) = %s".format(expected.dvce_ismobile) in {
      actual.dvce_ismobile must_== expected.dvce_ismobile
    }.pendingUntilFixed // nl.bitwalker.useragentutils is not parsing this user agent correctly
    "have dvce_ismobile_bt (Device Is Mobile, Byte?) = %s".format(expected.dvce_ismobile_bt) in {
      actual.dvce_ismobile_bt must_== expected.dvce_ismobile_bt
    }.pendingUntilFixed // nl.bitwalker.useragentutils is not parsing this user agent correctly

    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(expected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== expected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(expected.dvce_screenheight) in {
      actual.dvce_screenheight must_== expected.dvce_screenheight
    }
  }
}
