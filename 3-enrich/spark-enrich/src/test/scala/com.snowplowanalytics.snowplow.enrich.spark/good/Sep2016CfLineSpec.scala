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

object Sep2016CfLineSpec {
  import EnrichJobSpec._
  // September 2016: addition of cs-protocol-version (e.g. HTTP/1.1), 24 fields
  val lines = Lines(
    "2017-09-26	10:55:55	DUB2	474	255.255.255.255	GET	d3isjmrsi662be.cloudfront.net	/i	/200	-	Ruby	e=pv&dtm=1506423355307&p=srv&tv=rb-0.5.2&aid=sp&eid=2e9069a1-3f11-4770-b549-d8531c9f612e	-	Hit	tLCaixKW9q-oodVmuG4SbtljvWqLvVvJne8Nwt8O8DduCG8Z3vjifw==	d3isjmrsi662be.cloudfront.net	http	3574	0.001	-	-	-	Hit	HTTP/1.1"
  )
  val expected = List(
    "sp",
    "srv",
    etlTimestamp,
    "2017-09-26 10:55:55.000",
    "2017-09-26 10:55:55.307",
    "page_view",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "rb-0.5.2",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "255.255.255.255",
    null, // no user fp
    null, // no domain user id
    null, // no domain session id
    null, // No network_userid set
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
    null, // no page url
    null, // no page title
    null, // no page referer
    null, // no url scheme
    null, // no host
    null, // no port
    null, // no path
    null, // no url query
    null,
    null, // no referer scheme
    null, // no referer host
    null, // no referer port
    null, // no referer path
    null, // no referer url query
    null,
    null, // no referer type
    null, // no referer name
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event field empty
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
    "Ruby",
    "Unknown",
    "Unknown",
    null,
    "unknown",
    "OTHER",
    null, // no browser language
    null, // IE cannot report browser features
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // no browser cookies
    null, // no color depth
    null, // no view width
    null, // no view height
    "Unknown",
    "Unknown",
    "Other",
    null, // no os tz
    "Unknown",
    "0",
    null, // no device screen width
    null, // no device screen height
    null, // no doc charset
    null, // no doc width
    null, // no doc height
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Other","useragentMajor":null,"useragentMinor":null,"useragentPatch":null,"useragentVersion":"Other","osFamily":"Other","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Other","deviceFamily":"Other"}}]}""",
    null
  )
}

/**
 * Check that all tuples in a page view in the CloudFront format changed in September 2016
 * are successfully extracted.
 */
class Sep2016CfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "sep-2016-cf-lines"
  sequential
  "A job which processes a sep 2016 CloudFront file containing 1 valid page view" should {
    runEnrichJob(Sep2016CfLineSpec.lines, "cloudfront", "1", false, List("geo"))

    "correctly output 1 page ping" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- Sep2016CfLineSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(Sep2016CfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
