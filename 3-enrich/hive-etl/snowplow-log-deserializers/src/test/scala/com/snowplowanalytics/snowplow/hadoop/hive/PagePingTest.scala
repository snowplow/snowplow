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

// Specs2
import org.specs2.mutable.Specification

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Deserializer
import test.{SnowPlowDeserializer, SnowPlowEvent, SnowPlowTest}

class PagePingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Transaction item
  val row = "2012-05-27  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=pp&page=Async%20Test&pp_mix=28&pp_max=109&pp_miy=12&pp_may=22&tid=405338&vp=479x161&ds=584x193&p=web&tv=js-0.10.0&fp=119997519&aid=CFe23a&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=2560x1336&cd=24&cookie=1&url=file%3A%2F%2F%2Fhome%2Falex%2FDevelopment%2FSnowPlow%2Fsnowplow%2F1-trackers%2Fjavascript-tracker%2Fexamples%2Fweb%2Fasync.html"
  val expected = new SnowPlowEvent().tap { e =>
    e.dt = "2012-05-27"
    e.collector_tm = "11:35:53"
    e.event = "page_ping" // Page ping
    e.event_vendor = "com.snowplowanalytics"
    e.txn_id = "405338"
    e.page_url = "http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5"
    e.page_title = "Async Test"
    e.pp_xoffset_min = 28
    e.pp_xoffset_max = 109
    e.pp_yoffset_min = 12
    e.pp_yoffset_max = 22   
  }

  "The SnowPlow page ping row \"%s\"".format(row) should {

    val actual = SnowPlowDeserializer.deserialize(row)

    // General fields
    "have dt (Legacy Hive Date) = %s".format(expected.dt) in {
      actual.dt must_== expected.dt
    }
    "have collector_tm (Collector Time) = %s".format(expected.collector_tm) in {
      actual.collector_tm must_== expected.collector_tm
    }
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

    // Page fields
    "have page_url (Page URL) = %s".format(expected.page_url) in {
      actual.page_url must_== expected.page_url
    }
    "have page_title (Page Title) = %s".format(expected.page_title) in {
      actual.page_title must_== expected.page_title
    }

    // The page ping fields
    "have pp_xoffset_min (Page Ping Minimum X Offset) = %s".format(expected.pp_xoffset_min) in {
      actual.pp_xoffset_min must_== expected.pp_xoffset_min
    }
    "have pp_xoffset_max (Page Ping Maximum X Offset) = %s".format(expected.pp_xoffset_max) in {
      actual.pp_xoffset_max must_== expected.pp_xoffset_max
    }
    "have pp_yoffset_min (Page Ping Minimum Y Offset) = %s".format(expected.pp_yoffset_min) in {
      actual.pp_yoffset_min must_== expected.pp_yoffset_min
    }
    "have pp_yoffset_max (Page Ping Maximum Y Offset) = %s".format(expected.pp_yoffset_max) in {
      actual.pp_yoffset_max must_== expected.pp_yoffset_max
    }
  }
}