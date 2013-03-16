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

class CljTomcatFormatTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Input
  val row = "2012-12-03 04:49:53  - 37  127.0.0.1 GET localhost /i  200 http://yalisassoon.github.com/cl-collector-tests/async.html Mozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%3B+rv%3A16.0%29+Gecko%2F20100101+Firefox%2F16.0  ?ev_ca=Mixes&ev_ac=Play&ev_la=MRC%2Ffabric-0503-mix&ev_va=0.0&p=web&tid=755049&duid=915bc1dd0a4c5ba5&fp=2196241488&vid=3&tv=js-0.8.0&lang=en-GB&refr=http%3A%2F%2Fyalisassoon.github.com%2F&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&tz=Europe%2FLondon&url=http%3A%2F%2Fyalisassoon.github.com%2Fcl-collector-tests%2Fasync.html&nuid=7fc17b64-b202-46e4-8d3a-4d144edf2b23  - - -"

  // Output
  val expected = new SnowPlowEvent().tap { e =>
    e.dt = "2012-12-03"
    e.collector_tm = "04:49:53"
    e.txn_id = "755049"
    e.domain_userid = "915bc1dd0a4c5ba5"
    e.network_userid = "7fc17b64-b202-46e4-8d3a-4d144edf2b23"
    e.user_ipaddress = "127.0.0.1"
    e.domain_sessionidx = 3
    e.page_url = "http://yalisassoon.github.com/cl-collector-tests/async.html"
    e.page_referrer = "http://yalisassoon.github.com/"
    e.br_name = "Firefox"
    e.br_family = "Firefox"
    e.br_version = null // Our current parser lib couldn't figure out this version
    e.br_type = "Browser"
    e.br_renderengine = "GECKO"
    e.br_lang = "en-GB"
    e.br_cookies = true
    e.br_features = List("pdf", "qt", "ag", "java", "fla", "wma")
    e.os_name = "Windows"
    e.os_family = "Windows"
    e.os_manufacturer = "Microsoft Corporation"
    e.dvce_type = "Computer"
    e.dvce_ismobile = false
    e.dvce_screenwidth = 1920
    e.dvce_screenheight = 1080
  }

  "The SnowPlow page view row \"%s\" in the Clojure-Tomcat format".format(row) should {

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
    "have network_userid (Network User ID) = %s".format(expected.network_userid) in {
      actual.network_userid must_== expected.network_userid
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