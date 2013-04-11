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

class TransactionTest extends Specification {

	// Toggle if tests are failing and you want to inspect the struct contents
	implicit val _DEBUG = false

	// Transaction
	val row = "2012-05-24	11:35:53	DFW3	3343	99.116.172.58 GET d3gs014xn8p70.cloudfront.net	/ice.png	200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0	&e=tr&tr_id=order-123&tr_af=psychicbazaar&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&tid=028288&duid=a279872d76480afb&vid=1&aid=CFe23a&lang=en-GB&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1&url=file%3A%2F%2F%2Fhome%2Falex%2Fasync.html"
	val expected = new SnowPlowEvent().tap { e =>
		e.dt = "2012-05-24"
		e.collector_dt = "2012-05-24"
		e.collector_tm = "11:35:53"
		e.event = "transaction"
		e.txn_id = "028288"
		e.tr_orderid = "order-123"
		e.tr_affiliation = "psychicbazaar"
		e.tr_total = "8000"
		e.tr_tax = "200"
		e.tr_shipping = "50"
		e.tr_city = "London"
		e.tr_state = "England"
		e.tr_country = "UK"
	}

	"The SnowPlow ecommerce transaction row \"%s\"".format(row) should {

		val actual = SnowPlowDeserializer.deserialize(row)

		// General fields
		"have dt (Legacy Hive Date) = %s".format(expected.dt) in {
			actual.dt must_== expected.dt
		}
	    "have collector_dt (Collector Date) = %s".format(expected.collector_dt) in {
	      actual.collector_dt must_== expected.collector_dt
	    }
		"have collector_tm (Collector Time) = %s".format(expected.collector_tm) in {
			actual.collector_tm must_== expected.collector_tm
		}
	    "have a valid (stringly-typed UUID) event_id" in {
	      SnowPlowTest.stringlyTypedUuid(actual.event_id) must_== actual.event_id
	    }
		"have txn_id (Transaction ID) = %s".format(expected.txn_id) in {
			actual.txn_id must_== expected.txn_id
		}

		// The ecommerce transaction fields
		"have tr_orderid (Transaction Order ID) = %s".format(expected.tr_orderid) in {
			actual.tr_orderid must_== expected.tr_orderid
		}
		"have tr_affiliation (Transaction Affiliation) = %s".format(expected.tr_affiliation) in {
			actual.tr_affiliation must_== expected.tr_affiliation
		}
		"have tr_total (Transaction Total) = %s".format(expected.tr_total) in {
			actual.tr_total must_== expected.tr_total
		}
		"have tr_tax (Transaction Tax) = %s".format(expected.tr_tax) in {
			actual.tr_tax must_== expected.tr_tax
		}
		"have tr_shipping (Transaction Shipping) = %s".format(expected.tr_shipping) in {
			actual.tr_shipping must_== expected.tr_shipping
		}
		"have tr_city (Transaction City) = %s".format(expected.tr_city) in {
			actual.tr_city must_== expected.tr_city
		}
		"have tr_state (Transaction State) = %s".format(expected.tr_state) in {
			actual.tr_state must_== expected.tr_state
		}
		"have tr_country (Transaction Country) = %s".format(expected.tr_country) in {
			actual.tr_country must_== expected.tr_country
		}
	}
}