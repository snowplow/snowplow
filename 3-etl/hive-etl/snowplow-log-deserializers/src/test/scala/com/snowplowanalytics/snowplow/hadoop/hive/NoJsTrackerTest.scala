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

class NoJsTrackerTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row = "2012-05-24	11:35:53	DFW3	3343	99.116.172.58 GET d3gs014xn8p70.cloudfront.net	/ice.png	200 - Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0	&e=pv&page=root%20readme&url=https%3A%2F%2Fgithub.com%2Fsnowplow%2Fsnowplow&aid=snowplow&p=web&tv=no-js-0.1.0"
  
  val expected = new SnowPlowEvent().tap { e =>
    e.app_id = "snowplow"
    e.platform = "web"
    e.dt = "2012-05-24"
    e.collector_tm = "11:35:53"
    e.event = "page_view"
    e.page_title = "root readme"
    e.page_url = "https://github.com/snowplow/snowplow"
  }

  "The SnowPlow page view row from the No-JS tracker \"%s\"".format(row) should {

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
	  "have collector_tm (Collector Time) = %s".format(expected.collector_tm) in {
	    actual.collector_tm must_== expected.collector_tm
	  }

	  // Event and transaction
	  "have event (Event Type) = %s".format(expected.event) in {
	    actual.event must_== expected.event
	  }
	  "have a valid (stringly-typed UUID) event_id" in {
	    SnowPlowTest.stringlyTypedUuid(actual.event_id) must_== actual.event_id
	  }

	  // Page
	  "have page_url (Page URL) = %s".format(expected.page_url) in {
	    actual.page_url must_== expected.page_url
	  }
	  // Tracking a page view, so we have a page title
	  "have page_title (Page Title) = %s".format(expected.page_title) in {
	    actual.page_title must_== expected.page_title
	  }


  }
}