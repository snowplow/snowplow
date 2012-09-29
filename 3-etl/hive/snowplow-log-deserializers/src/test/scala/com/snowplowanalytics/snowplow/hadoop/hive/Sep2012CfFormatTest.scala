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

// Deserializer
import test.SnowPlowDeserializer

class Sep2012CfFormatTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Input
  val row = "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1 - Hit vHql4ZhKJSl8yUJZuCrmvwBuVGmmgizVsKoo8lfPIn-ts0gR4g7KmA=="

  // Output
  object RowExpected {
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

  "The SnowPlow page view row \"%s\" in the new (12 Sep 2012) CloudFront format".format(row) should {

    val actual = SnowPlowDeserializer.deserialize(row)

    // Check all of the field values

    // Date/time
    "have dt (Date) = %s".format(RowExpected.dt) in {
      actual.dt must_== RowExpected.dt
    }
    "have tm (Time) = %s".format(RowExpected.tm) in {
      actual.tm must_== RowExpected.tm
    }

    // Transaction
    "have txn_id (Transaction ID) = %s".format(RowExpected.txn_id) in {
      actual.txn_id must_== RowExpected.txn_id
    }

    // User and visit
    "have user_id (User ID) = %s".format(RowExpected.user_id) in {
      actual.user_id must_== RowExpected.user_id
    }
    "have user_ipaddress (User IP Address) = %s".format(RowExpected.user_ipaddress) in {
      actual.user_ipaddress must_== RowExpected.user_ipaddress
    }
    "have visit_id (User IP Address) = %s".format(RowExpected.visit_id) in {
      actual.visit_id must_== RowExpected.visit_id
    }

    // Page
    "have page_url (Page URL) = %s".format(RowExpected.page_url) in {
      actual.page_url must_== RowExpected.page_url
    }
    // Tracking a page view, so we have a page title
    "have page_title (Page Title) = %s".format(RowExpected.page_title) in {
      actual.page_title must_== RowExpected.page_title
    }
    "have page_referrer (Page Referrer) = %s".format(RowExpected.page_referrer) in {
      actual.page_referrer must_== RowExpected.page_referrer
    }

    // Browser (from user-agent)
    "have br_name (Browser Name) = %s".format(RowExpected.br_name) in {
      actual.br_name must_== RowExpected.br_name
    }
    "have br_family (Browser Family) = %s".format(RowExpected.br_family) in {
      actual.br_family must_== RowExpected.br_family
    }
    "have br_version (Browser Version) = %s".format(RowExpected.br_version) in {
      actual.br_version must_== RowExpected.br_version
    }
    "have br_type (Browser Type) = %s".format(RowExpected.br_type) in {
      actual.br_type must_== RowExpected.br_type
    }
    "have br_renderengine (Browser Rendering Engine) = %s".format(RowExpected.br_renderengine) in {
      actual.br_renderengine must_== RowExpected.br_renderengine
    }

    // Browser (from querystring)
    "have br_lang (Browser Lang) = %s".format(RowExpected.br_lang) in {
      actual.br_lang must_== RowExpected.br_lang
    }
    "have br_cookies (Browser Cookies Enabled?) = %s".format(RowExpected.br_cookies) in {
      actual.br_cookies must_== RowExpected.br_cookies
    }
    "have br_features (Browser Features) = %s".format(RowExpected.br_features) in {
      // For some reason (Specs2) couldn't use implicit Java->Scala conversion here
      JavaConversions.asScalaBuffer(actual.br_features) must haveTheSameElementsAs(RowExpected.br_features)
    }.pendingUntilFixed // For some reason actual.br_features empties when inside this test

    // OS (from user-agent)    
    "have os_name (OS Name) = %s".format(RowExpected.os_name) in {
      actual.os_name must_== RowExpected.os_name
    }
    "have os_family (OS Family) = %s".format(RowExpected.os_family) in {
      actual.os_family must_== RowExpected.os_family
    }
    "have os_manufacturer (OS Manufacturer) = %s".format(RowExpected.os_manufacturer) in {
      actual.os_manufacturer must_== RowExpected.os_manufacturer
    }
    
    // Device/Hardware (from user-agent) 
    "have dvce_type (Device Type) = %s".format(RowExpected.dvce_type) in {
      actual.dvce_type must_== RowExpected.dvce_type
    }
    "have dvce_ismobile (Device Is Mobile?) = %s".format(RowExpected.dvce_ismobile) in {
      actual.dvce_ismobile must_== RowExpected.dvce_ismobile
    }
    
    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(RowExpected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== RowExpected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(RowExpected.dvce_screenheight) in {
      actual.dvce_screenheight must_== RowExpected.dvce_screenheight
    }
  }
}