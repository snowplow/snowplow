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

// Java
import java.util.{ArrayList => JArrayList}

// Scala
import scala.collection.JavaConversions

// Specs2
import org.specs2.mutable.Specification

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Deserializer
import test.{SnowPlowDeserializer, SnowPlowEvent}

class Sep2012CfFormatTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Input
  val row = "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&duid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=0&f_java=1&f_gears=0&f_ag=0&res=640x960&cookie=1 - Hit vHql4ZhKJSl8yUJZuCrmvwBuVGmmgizVsKoo8lfPIn-ts0gR4g7KmA=="

  // Output
  val expected = new SnowPlowEvent().tap { e =>
    e.dt = "2012-05-24"
    e.collector_tm = "00:08:40"
    e.txn_id = "721410"
    e.domain_userid = "3798cdce0493133e"
    e.user_ipaddress = "74.125.17.210"
    e.domain_sessionidx = 1
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
    e.br_features = List("java", "qt", "pdf")
    e.os_name = "Android"
    e.os_family = "Android"
    e.os_manufacturer = "Google Inc."
    e.dvce_type = "Mobile"
    e.dvce_ismobile = true
    e.dvce_screenwidth = 640
    e.dvce_screenheight = 960
  }

  "The SnowPlow page view row \"%s\" in the new (12 Sep 2012) CloudFront format".format(row) should {

    val actual = SnowPlowDeserializer.deserialize(row)

    // Check all of the field values

    // Date/time
    "have dt (Legacy Hive Date) = %s".format(expected.dt) in {
      actual.dt must_== expected.dt
    }
    "have collector_tm (Collector Time) = %s".format(expected.collector_tm) in {
      actual.collector_tm must_== expected.collector_tm
    }

    // Transaction
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
    "have br_features (Browser Features) = %s".format(expected.br_features) in {
      // For some reason (Specs2) couldn't use implicit Java->Scala conversion here
      JavaConversions.asScalaBuffer(actual.br_features) must haveTheSameElementsAs(expected.br_features)
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
    
    // Device/Hardware (from user-agent) 
    "have dvce_type (Device Type) = %s".format(expected.dvce_type) in {
      actual.dvce_type must_== expected.dvce_type
    }
    "have dvce_ismobile (Device Is Mobile?) = %s".format(expected.dvce_ismobile) in {
      actual.dvce_ismobile must_== expected.dvce_ismobile
    }
    
    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(expected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== expected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(expected.dvce_screenheight) in {
      actual.dvce_screenheight must_== expected.dvce_screenheight
    }
  }
}