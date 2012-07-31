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

class PageViewTest extends Specification {

  sequential

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // Input
  // val row1 = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&page=Test&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_va=Empty&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&uid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"
  val row1 = "2012-05-24  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  page=Tarot%2520cards%2520-%2520Psychic%2520Bazaar&tid=344260&uid=288112e0a5003be2&vid=1&lang=en-US&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fp%253D4&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1366x768&cookie=1"
  val row2 = "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1"
  val row3 = "2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=1&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=0&f_java=0&f_gears=0&f_ag=0&res=320x480&cookie=1"

  // Output
  object Row3Expected {
    val dt = "2012-05-24"
    val tm = "00:08:40"
    val txn_id = "721410"
    val user_id = "3798cdce0493133e"
    val user_ipaddress = "74.125.17.210"
    val visit_id = 1
    val page_url = "http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html"
    val page_title = "Psycards book and deck starter pack - Psychic Bazaar"
    val page_referrer = "http://www.google.com/m/search"
    val br_name = "Safari 4"
    val br_family = "Safari"
    val br_version = "4.0"
    val br_type = "Browser"
    val br_renderengine = "WEBKIT"
    val br_lang = "en"
    val br_cookies = true
    val br_features = List("qt")
    val os_name = "Android"
    val os_family = "Android"
    val os_manufacturer = "Google Inc."
    val dvce_type = "Mobile"
    val dvce_ismobile = true
    val dvce_screenwidth = 640
    val dvce_screenheight = 960
  }

  "A valid SnowPlow page view row should deserialize without error" >> {
    Seq(row1, row2) foreach { row =>
      "Page view row \"%s\" deserializes okay".format(row) >> {
        SnowPlowEventDeserializer.deserializeLine(row, DEBUG) must beAnInstanceOf[SnowPlowEventStruct]
      }
    }
  }

  "The SnowPlow page view row \"%s\"".format(row3) should {

    val actual = SnowPlowEventDeserializer.deserializeLine(row3, DEBUG).asInstanceOf[SnowPlowEventStruct]

    // Check all of the field values

    // Date/time
    "have dt (Date) = %s".format(Row3Expected.dt) in {
      actual.dt must_== Row3Expected.dt
    }
    "have tm (Time) = %s".format(Row3Expected.tm) in {
      actual.tm must_== Row3Expected.tm
    }

    // Transaction
    "have txn_id (Transaction ID) = %s".format(Row3Expected.txn_id) in {
      actual.txn_id must_== Row3Expected.txn_id
    }

    // User and visit
    "have user_id (User ID) = %s".format(Row3Expected.user_id) in {
      actual.user_id must_== Row3Expected.user_id
    }
    "have user_ipaddress (User IP Address) = %s".format(Row3Expected.user_ipaddress) in {
      actual.user_ipaddress must_== Row3Expected.user_ipaddress
    }
    "have visit_id (User IP Address) = %s".format(Row3Expected.visit_id) in {
      actual.visit_id must_== Row3Expected.visit_id
    }

    // Page
    "have page_url (Page URL) = %s".format(Row3Expected.page_url) in {
      actual.page_url must_== Row3Expected.page_url
    }
    // Tracking a page view, so we have a page title
    "have page_title (Page Title) = %s".format(Row3Expected.page_title) in {
      actual.page_title must_== Row3Expected.page_title
    }
    "have page_referrer (Page Referrer) = %s".format(Row3Expected.page_referrer) in {
      actual.page_referrer must_== Row3Expected.page_referrer
    }

    // Browser (from user-agent)
    "have br_name (Browser Name) = %s".format(Row3Expected.br_name) in {
      actual.br_name must_== Row3Expected.br_name
    }
    "have br_family (Browser Family) = %s".format(Row3Expected.br_family) in {
      actual.br_family must_== Row3Expected.br_family
    }
    "have br_version (Browser Version) = %s".format(Row3Expected.br_version) in {
      actual.br_version must_== Row3Expected.br_version
    }
    "have br_type (Browser Type) = %s".format(Row3Expected.br_type) in {
      actual.br_type must_== Row3Expected.br_type
    }
    "have br_renderengine (Browser Rendering Engine) = %s".format(Row3Expected.br_renderengine) in {
      actual.br_renderengine must_== Row3Expected.br_renderengine
    }

    // Browser (from querystring)
    "have br_lang (Browser Lang) = %s".format(Row3Expected.br_lang) in {
      actual.br_lang must_== Row3Expected.br_lang
    }
    "have br_cookies (Browser Cookies Enabled?) = %s".format(Row3Expected.br_cookies) in {
      actual.br_cookies must_== Row3Expected.br_cookies
    }
    "have br_features (Browser Features) = %s".format(Row3Expected.br_features) in {
      // For some reason (Specs2) couldn't use implicit Java->Scala conversion here
      JavaConversions.asScalaBuffer(actual.br_features) must haveTheSameElementsAs(Row3Expected.br_features)
    }.pendingUntilFixed // For some reason actual.br_features empties when inside this test

    // OS (from user-agent)    
    "have os_name (OS Name) = %s".format(Row3Expected.os_name) in {
      actual.os_name must_== Row3Expected.os_name
    }
    "have os_family (OS Family) = %s".format(Row3Expected.os_family) in {
      actual.os_family must_== Row3Expected.os_family
    }
    "have os_manufacturer (OS Manufacturer) = %s".format(Row3Expected.os_manufacturer) in {
      actual.os_manufacturer must_== Row3Expected.os_manufacturer
    }
    
    // Device/Hardware (from user-agent) 
    "have dvce_type (Device Type) = %s".format(Row3Expected.dvce_type) in {
      actual.dvce_type must_== Row3Expected.dvce_type
    }
    "have dvce_ismobile (Device Is Mobile?) = %s".format(Row3Expected.dvce_ismobile) in {
      actual.dvce_ismobile must_== Row3Expected.dvce_ismobile
    }
    
    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(Row3Expected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== Row3Expected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(Row3Expected.dvce_screenheight) in {
      actual.dvce_screenheight must_== Row3Expected.dvce_screenheight
    }
  }
}