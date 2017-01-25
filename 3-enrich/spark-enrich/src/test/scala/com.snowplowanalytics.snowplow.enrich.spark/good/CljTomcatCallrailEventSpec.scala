/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark
package good

import org.specs2.mutable.Specification

object CljTomcatCallrailEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.callrail/v1    404 -   -   aid=bnb&answered=true&callercity=BAKERSFIELD&callercountry=US&callername=SKYPE+CALLER&callernum=%2B16617240240&callerstate=CA&callerzip=93307&callsource=keyword&datetime=2014-10-09+16%3A23%3A45&destinationnum=2015014231&duration=247&first_call=true&ga=&gclid=&id=305895151&ip=86.178.183.7&keywords=&kissmetrics_id=&landingpage=http%3A%2F%2Flndpage.com%2F&recording=http%3A%2F%2Fapp.callrail.com%2Fcalls%2F305895151%2Frecording%2F9f59ad59ba1cfa964372&referrer=direct&referrermedium=Direct&trackingnum=%2B12015911668&transcription=&utm_campaign=&utm_content=&utm_medium=&utm_source=&utm_term=&utma=&utmb=&utmc=&utmv=&utmx=&utmz=&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -"
  )

  val expected = List(
    "bnb",
    "srv",
    etlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.callrail-v1",
    "clj-0.6.0-tom-0.0.4",
    etlVersion,
    null, // No user_id set
    "255.255.x.x",
    null,
    null,
    null,
    "-", // TODO: fix this, https://github.com/snowplow/snowplow/issues/1133
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.callrail/call_complete/jsonschema/1-0-0","data":{"duration":247,"utm_source":null,"utmv":null,"ip":"86.178.183.7","utmx":null,"ga":null,"destinationnum":"2015014231","datetime":"2014-10-09T16:23:45.000Z","kissmetrics_id":null,"landingpage":"http://lndpage.com/","callerzip":"93307","gclid":null,"callername":"SKYPE CALLER","utmb":null,"id":"305895151","callernum":"+16617240240","utm_content":null,"trackingnum":"+12015911668","referrermedium":"Direct","utm_campaign":null,"keywords":null,"transcription":null,"utmz":null,"utma":null,"referrer":"direct","callerstate":"CA","recording":"http://app.callrail.com/calls/305895151/recording/9f59ad59ba1cfa964372","first_call":true,"utmc":null,"callercountry":"US","utm_medium":null,"callercity":"BAKERSFIELD","utm_term":null,"answered":true,"callsource":"keyword"}}}""", // Unstructured event field empty
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )
}

/**
 * Check that all tuples in a CallRail completed call event in the Clojure-Tomcat format are
 * successfully extracted.
 * For details: https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class CljTomcatCallrailEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-callrail-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a GET raw event representing 1 valid " +
  "completed call" should {
    runEnrichJob(CljTomcatCallrailEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatCallrailEventSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(CljTomcatCallrailEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
