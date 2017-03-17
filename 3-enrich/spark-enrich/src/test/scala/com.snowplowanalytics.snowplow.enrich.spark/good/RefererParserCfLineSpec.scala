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

object RefererParserCfLineSpec {
  import EnrichJobSpec._
  // April 2014: Amazon added a new field (time-taken) to the end of the Access Log format
  val lines = Lines(
    "2014-04-29 09:00:54    CDG51   830 255.255.255.255 GET d3v6ndkyapxc2w.cloudfront.net   /i  200 http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/   Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36    e=pp&page=Loading%2520JSON%2520data%2520into%2520Redshift%2520-%2520the%2520challenges%2520of%2520quering%2520JSON%2520data%252C%2520and%2520how%2520Snowplow%2520can%2520be%2520used%2520to%2520meet%2520those%2520challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJkYXRhIjpbeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91cmlfcmVkaXJlY3QvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsidXJpIjoiaHR0cDovL3Nub3dwbG93YW5hbHl0aWNzLmNvbS8ifX1dLCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIn0=&dtm=1398762054889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%252FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fwww.subdomain1.snowplowanalytics.com%252Fservices%252Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fblog%252F2013%252F11%252F20%252Floading-json-data-into-redshift%252F%2523weaknesses -   Hit cN-iKWE_3tTwxIKGkSnUOjGpmNjsDUyk4ctemoxU_zIG7Md_fH87sg==    d3v6ndkyapxc2w.cloudfront.net   http    1163    0.001"
  )
  val expected = List(
    "snowplowweb",
    "web",
    etlTimestamp,
    "2014-04-29 09:00:54.000",
    "2014-04-29 09:00:54.889",
    "page_ping",
    null, // We can't predict the event_id
    "612876",
    "cloudfront", // Tracker namespace
    "js-2.0.0",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "255.255.255.255",
    "2071613637",
    "44082d3af0e30126",
    "2",
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
    "http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/#weaknesses",
    "Loading JSON data into Redshift - the challenges of quering JSON data, and how Snowplow can be used to meet those challenges",
    "http://www.subdomain1.snowplowanalytics.com/services/pipelines.html",
    "http",
    "snowplowanalytics.com",
    "80",
    "/blog/2013/11/20/loading-json-data-into-redshift/",
    null,
    "weaknesses",
    "http",
    "www.subdomain1.snowplowanalytics.com",
    "80",
    "/services/pipelines.html",
    null,
    null,
    "internal", // www.subdomain1.snowplowanalytics.com counts as an internal referer
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    """{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"http://snowplowanalytics.com/"}}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""",
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
    "0", // Page ping fields
    "1", //
    "64", //
    "935", //
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome 34",
    "Chrome",
    "34.0.1847.131",
    "Browser",
    "WEBKIT",
    "fr",
    "1",
    "1",
    "1",
    "0",
    "1",
    "0",
    "0",
    "0",
    "0",
    "1",
    "24",
    "1279",
    "610",
    "Mac OS X",
    "Mac OS X",
    "Apple Inc.",
    "Europe/Berlin",
    "Computer",
    "0",
    "1280",
    "800",
    "UTF-8",
    "1279",
    "5614"
  )
}

/**
 * Check that a referer with one of the user-specified internal domain names is counted as internal
 */
class RefererParserCfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "referer-parser-cf-lines"
  sequential
  "A job which processes a CloudFront file containing 1 valid page ping with an internal " +
  "subdomain referer" should {
    runEnrichJob(RefererParserCfLineSpec.lines, "cloudfront", "1", false, List("geo"))

    "correctly output 1 page ping" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- RefererParserCfLineSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(RefererParserCfLineSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
