/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
import test.{SnowPlowDeserializer, SnowPlowEvent, SnowPlowTest}

class PageViewTest2 extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row = "2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  e=pv&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&vp=479x283&ds=584x268&cs=UTF-8&duid=e7bccbb647296c98&vid=1&p=Web&aid=CFe23a&fp=1906624389&tz=Europe%2FLondon&cd=24&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1"

  val expected = new SnowPlowEvent().tap { e =>
    e.app_id = "CFe23a"
    e.platform = "Web"
    e.dt = "2012-05-24"
    e.collector_dt = "2012-05-24"
    e.collector_tm = "00:06:42"
    e.event = "page_view"
    e.event_vendor = "com.snowplowanalytics"
    e.txn_id = "019539"
    e.domain_userid = "e7bccbb647296c98"
    e.user_ipaddress = "90.194.12.51"
    e.user_fingerprint = "1906624389"
    e.domain_sessionidx = 1
    e.page_url = "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail"
    e.page_title = "Psycards book and deck starter pack - Psychic Bazaar"
    e.page_referrer = "http://www.google.com/search?hl=en&q=the+psycard+story&oq=the+psycard+story&aq=f&aqi=&aql=&gs_l=mobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ&mvs=0"
    e.page_urlscheme = "http"
    e.page_urlhost = "www.psychicbazaar.com"
    e.page_urlport = 80
    e.page_urlpath = "/oracles/119-psycards-book-and-deck-starter-pack.html"
    e.page_urlquery = "view=print"
    e.page_urlfragment = "detail"
    e.useragent = "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3"
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
    e.br_colordepth = "24"
    e.br_viewwidth = 479
    e.br_viewheight = 283
    e.os_name = "Mac OS" // Yech, nl.bitwalker.useragentutils is not parsing this user agent correctly
    e.os_family = "Mac OS" // Yech, nl.bitwalker.useragentutils is not parsing this user agent correctly
    e.os_manufacturer = "Apple Inc."
    e.os_timezone = "Europe/London"
    e.dvce_type = "Computer" // Yech, nl.bitwalker.useragentutils is not parsing this user agent correctly
    e.dvce_ismobile = false // Yech, nl.bitwalker.useragentutils is not parsing this user agent correctly
    e.dvce_ismobile_bt = 0 // Yech, nl.bitwalker.useragentutils is not parsing this user agent correctly
    e.dvce_screenwidth = 320
    e.dvce_screenheight = 480
    e.doc_charset = "UTF-8"
    e.doc_width = 584
    e.doc_height = 268
  }

  "The SnowPlow page view row \"%s\"".format(row) should {

    val actual = SnowPlowDeserializer.deserialize(row)

    // Check all of the field values

    // The application (site, game, app etc) this event belongs to, and the tracker platform
    "have app_id (Application ID) = %s".format(expected.app_id) in {
      actual.app_id must_== expected.app_id
    }
    "have platform (Platform) = %s".format(expected.platform) in {
      actual.platform must_== expected.platform
    }

    // Date/time
    "have dt (Legacy Hive Date) = %s".format(expected.dt) in {
      actual.dt must_== expected.dt
    }
    "have collector_dt (Collector Date) = %s".format(expected.collector_dt) in {
      actual.collector_dt must_== expected.collector_dt
    }
    "have collector_tm (Collector Time) = %s".format(expected.collector_tm) in {
      actual.collector_tm must_== expected.collector_tm
    }

    // Event and transaction
    "have event (Event Type) = %s".format(expected.event) in {
      actual.event must_== expected.event
    }
    "have event_vendor (Event Vendor) = %s".format(expected.event_vendor) in {
      actual.event_vendor must_== expected.event_vendor
    }
    "have a valid (stringly-typed UUID) event_id" in {
      SnowPlowTest.stringlyTypedUuid(actual.event_id) must_== actual.event_id
    }
    "have txn_id (Transaction ID) = %s".format(expected.txn_id) in {
      actual.txn_id must_== expected.txn_id
    }

    // User and visit
    "have domain_userid (Domain User ID) = %s".format(expected.domain_userid) in {
      actual.domain_userid must_== expected.domain_userid
    }
    "have user_ipaddress (User IP Address) = %s".format(expected.user_ipaddress) in {
      actual.user_ipaddress must_== expected.user_ipaddress
    }
    "have user_fingerprint (User Fingerprint) = %s".format(expected.user_fingerprint) in {
      actual.user_fingerprint must_== expected.user_fingerprint
    }
    "have visit_id (User IP Address) = %s".format(expected.domain_sessionidx) in {
      actual.domain_sessionidx must_== expected.domain_sessionidx
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

    // Page URL components
    "have page_urlscheme (Page URL) = %s".format(expected.page_urlscheme) in {
      actual.page_urlscheme must_== expected.page_urlscheme
    }
    // Tracking a page view, so we have a page title
    "have page_urlhost (Page URL Host) = %s".format(expected.page_urlhost) in {
      actual.page_urlhost must_== expected.page_urlhost
    }
    "have page_urlport (Page URL Port) = %s".format(expected.page_urlport) in {
      actual.page_urlport must_== expected.page_urlport
    }
    "have page_urlpath (Page URL Path) = %s".format(expected.page_urlpath) in {
      actual.page_urlpath must_== expected.page_urlpath
    }
    "have page_urlquery (Page URL Querystring) = %s".format(expected.page_urlquery) in {
      actual.page_urlquery must_== expected.page_urlquery
    }
    "have page_urlfragment (Page URL Fragment aka Anchor) = %s".format(expected.page_urlfragment) in {
      actual.page_urlfragment must_== expected.page_urlfragment
    }

    // Useragent
    "have useragent (User Agent) = %s".format(expected.useragent) in {
      actual.useragent must_== expected.useragent
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
    "have br_colordepth (Browser Color Depth) = %s".format(expected.br_colordepth) in {
      actual.br_colordepth must_== expected.br_colordepth
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
    "have br_viewwidth (Viewport Width) = %s".format(expected.br_viewwidth) in {
      actual.br_viewwidth must_== expected.br_viewwidth
    }
    "have br_viewheight (Viewport Height) = %s".format(expected.br_viewheight) in {
      actual.br_viewheight must_== expected.br_viewheight
    }

    // OS (from user-agent)
    "have os_name (OS Name) = %s".format(expected.os_name) in {
      actual.os_name must_== expected.os_name
    }
    "have os_family (OS Family) = %s".format(expected.os_family) in {
      actual.os_family must_== expected.os_family
    }
    "have os_manufacturer (OS Manufacturer) = %s".format(expected.os_manufacturer) in {
      actual.os_manufacturer must_== expected.os_manufacturer
    }
    "have os_timezone (OS Timezone) = %s".format(expected.os_timezone) in {
      actual.os_timezone must_== expected.os_timezone
    }

    // Device/Hardware (from user-agent)
    "have dvce_type (Device Type) = %s".format(expected.dvce_type) in {
      actual.dvce_type must_== expected.dvce_type
    }
    "have dvce_ismobile (Device Is Mobile?) = %s".format(expected.dvce_ismobile) in {
      actual.dvce_ismobile must_== expected.dvce_ismobile
    }
    "have dvce_ismobile_bt (Device Is Mobile, Byte?) = %s".format(expected.dvce_ismobile_bt) in {
      actual.dvce_ismobile_bt must_== expected.dvce_ismobile_bt
    }

    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(expected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== expected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(expected.dvce_screenheight) in {
      actual.dvce_screenheight must_== expected.dvce_screenheight
    }

    // Document fields
    "have doc_charset (Document character set) = %s".format(expected.doc_charset) in {
      actual.doc_charset must_== expected.doc_charset
    }
    "have doc_width (Document Width) = %s".format(expected.doc_width) in {
      actual.doc_width must_== expected.doc_width
    }
    "have doc_height (Document Height) = %s".format(expected.doc_height) in {
      actual.doc_height must_== expected.doc_height
    }
  }
}
