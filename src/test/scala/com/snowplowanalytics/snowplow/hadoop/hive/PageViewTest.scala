/*
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
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

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

class PageViewTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // Input
  val row = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&page=Test&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_va=Empty&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&uid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"
  
  // Output
  object Expected {
    val dt = ""
    val tm = ""
    val txn_id = ""
    val user_id = ""
    val user_ipaddress = ""
    val visit_id = ""
    val page_url = ""
    val page_title = ""
    val page_referrer = ""
    val br_name = ""
    val br_family = ""
    val br_version = ""
    val br_type = ""
    val br_renderengine = ""
    val br_lang = ""
    val br_cookies = ""
    val br_features = ""
    val os_name = ""
    val os_family = ""
    val os_manufacturer = ""
    val dvce_type = ""
    val dvce_ismobile = ""
    val dvce_screenwidth = ""
    val dvce_screenheight = ""
  }

  "The SnowPlow page view record \"%s\"".format(row) should {

    val actual = SnowPlowEventDeserializer.deserializeLine(row, DEBUG).asInstanceOf[SnowPlowEventStruct]

    // Check all of the field values

    // Date/time
    "have dt (Date) = %s".format(Expected.dt) in {
      actual.dt must_== Expected.dt
    }
    "have tm (Time) = %s".format(Expected.tm) in {
      actual.tm must_== Expected.tm
    }

    // Transaction
    "have txn_id (Transaction ID) = %s".format(Expected.txn_id) in {
      actual.txn_id must_== Expected.txn_id
    }

    // User and visit
    "have user_id (User ID) = %s".format(Expected.user_id) in {
      actual.user_id must_== Expected.user_id
    }
    "have user_ipaddress (User IP Address) = %s".format(Expected.user_ipaddress) in {
      actual.user_ipaddress must_== Expected.user_ipaddress
    }
    "have visit_id (User IP Address) = %s".format(Expected.visit_id) in {
      actual.visit_id must_== Expected.visit_id
    }

    // Page
    "have page_url (Page URL) = %s".format(Expected.page_url) in {
      actual.page_url must_== Expected.page_url
    }
    // Tracking a page view, so we have a page title
    "have page_title (Page Title) = %s".format(Expected.page_title) in {
      actual.page_title must_== Expected.page_title
    }
    "have page_referrer (Page Referrer) = %s".format(Expected.page_referrer) in {
      actual.page_referrer must_== Expected.page_referrer
    }

    // Browser (from user-agent)
    "have br_name (Browser Name) = %s".format(Expected.br_name) in {
      actual.br_name must_== Expected.br_name
    }
    "have br_family (Browser Family) = %s".format(Expected.br_family) in {
      actual.br_family must_== Expected.br_family
    }
    "have br_version (Browser Version) = %s".format(Expected.br_version) in {
      actual.br_version must_== Expected.br_version
    }
    "have br_type (Browser Type) = %s".format(Expected.br_type) in {
      actual.br_type must_== Expected.br_type
    }
    "have br_renderengine (Browser Rendering Engine) = %s".format(Expected.br_renderengine) in {
      actual.br_renderengine must_== Expected.br_renderengine
    }

    // Browser (from querystring)
    "have br_lang (Browser Lang) = %s".format(Expected.br_lang) in {
      actual.br_lang must_== Expected.br_lang
    }
    "have br_cookies (Browser Cookies Enabled?) = %s".format(Expected.br_cookies) in {
      actual.br_cookies must_== Expected.br_cookies
    }
    "have br_features (Browser Features) = %s".format(Expected.br_features) in {
      actual.br_features must_== Expected.br_features
    }

    // OS (from user-agent)    
    "have os_name (OS Name) = %s".format(Expected.os_name) in {
      actual.os_name must_== Expected.os_name
    }
    "have os_family (OS Family) = %s".format(Expected.os_family) in {
      actual.os_family must_== Expected.os_family
    }
    "have os_manufacturer (OS Manufacturer) = %s".format(Expected.os_manufacturer) in {
      actual.os_manufacturer must_== Expected.os_manufacturer
    }
    
    // Device/Hardware (from user-agent) 
    "have dvce_ismobile (Device Is Mobile?) = %s".format(Expected.dvce_ismobile) in {
      actual.dvce_ismobile must_== Expected.dvce_ismobile
    }
    "have dvce_type (Device Type) = %s".format(Expected.dvce_type) in {
      actual.dvce_type must_== Expected.dvce_type
    }
    
    // Device (from querystring)
    "have dvce_screenwidth (Device Screen Width) = %s".format(Expected.dvce_screenwidth) in {
      actual.dvce_screenwidth must_== Expected.dvce_screenwidth
    }
    "have dvce_screenheight (Device Screen Height) = %s".format(Expected.dvce_screenheight) in {
      actual.dvce_screenheight must_== Expected.dvce_screenheight
    }
  }
}