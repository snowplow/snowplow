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

object WebDistributionLoaderSpec {
  import EnrichJobSpec._
  // April 2014: Amazon added a new field (time-taken) to the end of the Access Log format
  val lines = Lines(
    "2014-04-29	09:00:54	CDG51	830	255.255.255.255	GET	d3v6ndkyapxc2w.cloudfront.net	/i	200	http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/	Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36	e=pp&page=Loading%2520JSON%2520data%2520into%2520Redshift%2520-%2520the%2520challenges%2520of%2520quering%2520JSON%2520data%252C%2520and%2520how%2520Snowplow%2520can%2520be%2520used%2520to%2520meet%2520those%2520challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJwYWdlIjp7InVybCI6ImJsb2cifX0&dtm=1398762054889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%252FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fservices%252Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fblog%252F2013%252F11%252F20%252Floading-json-data-into-redshift%252F%2523weaknesses	-	Hit	cN-iKWE_3tTwxIKGkSnUOjGpmNjsDUyk4ctemoxU_zIG7Md_fH87sg==	d3v6ndkyapxc2w.cloudfront.net	http	1163	0.001"
  )
  val expected = List(
    null,
    "srv",
    etlTimestamp,
    "2014-04-29 09:00:54.000",
    null,
    "unstruct",
    null,
    null,
    null,
    "com.amazon.aws.cloudfront/wd_access_log", // "tracker version"
    "tsv", // "collector version"
    etlVersion,
    null,
    "255.255.255.255",
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
    "http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/", // page_url
    null,
    null,
    "http", // page_urlscheme
    "snowplowanalytics.com", // page_urlhost:
    "80", // page_urlport
    "/blog/2013/11/20/loading-json-data-into-redshift/", // page_urlpath
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-3","data":{"dateTime":"2014-04-29T09:00:54Z","xEdgeLocation":"CDG51","scBytes":830,"cIp":"255.255.255.255","csMethod":"GET","csHost":"d3v6ndkyapxc2w.cloudfront.net","csUriStem":"/i","scStatus":"200","csReferer":"http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/","csUserAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36","csUriQuery":"e=pp&page=Loading%20JSON%20data%20into%20Redshift%20-%20the%20challenges%20of%20quering%20JSON%20data%2C%20and%20how%20Snowplow%20can%20be%20used%20to%20meet%20those%20challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJwYWdlIjp7InVybCI6ImJsb2cifX0&dtm=1398762054889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%2FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%3A%2F%2Fsnowplowanalytics.com%2Fservices%2Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2013%2F11%2F20%2Floading-json-data-into-redshift%2F%23weaknesses","csCookie":"-","xEdgeResultType":"Hit","xEdgeRequestId":"cN-iKWE_3tTwxIKGkSnUOjGpmNjsDUyk4ctemoxU_zIG7Md_fH87sg==","xHostHeader":"d3v6ndkyapxc2w.cloudfront.net","csProtocol":"http","csBytes":1163,"timeTaken":0.001}}}""",
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome 34",
    "Chrome",
    "34.0.1847.131",
    "Browser",
    "WEBKIT",
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
    "Mac OS X",
    "Mac OS X",
    "Apple Inc.",
    null,
    "Computer",
    "0",
    null,
    null,
    null,
    null,
    null
  )
}

/**
 * Check that log files for a Cloudfront web distribution are correctly processed
 * For details: https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class WebDistributionLoaderSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "web-distribution-loader"
  sequential
  "A job which processes a CloudFront web distribution log file" should {
    runEnrichJob(WebDistributionLoaderSpec.lines, "tsv/com.amazon.aws.cloudfront/wd_access_log",
      "1", false, List("geo"))
    "correctly output 1 page ping" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- WebDistributionLoaderSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(WebDistributionLoaderSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
