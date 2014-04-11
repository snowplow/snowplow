/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package good

// Scala
import scala.collection.mutable.Buffer

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatLineSpec {

  val lines = Lines(
    "2013-10-07	19:47:54	-	37	255.255.255.255	GET	255.255.255.255	/i	200	http://snowplowanalytics.com/blog/2012/10/31/snowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow/	Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10_6_8%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F31.0.1650.8+Safari%2F537.36	e=pv&page=Snowplow%20in%20a%20Universal%20Analytics%20world%20-%20what%20the%20new%20version%20of%20Google%20Analytics%20means%20for%20companies%20adopting%20Snowplow%20-%20Snowplow%20Analytics&dtm=1381175274123&tid=958446&vp=1440x802&evn=com.snowplowanalytics&ds=1425x4674&vid=1&duid=d159c05f2aa8e1b9&p=web&tv=js-0.12.0&fp=812263905&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=Europe%2FLondon&refr=https%3A%2F%2Fwww.google.co.uk%2Furl%3Fsa%3Dt%26rct%3Dj%26q%3D%26esrc%3Ds%26source%3Dweb%26cd%3D3%26ved%3D0CDsQFjAC%26url%3Dhttp%253A%252F%252Fsnowplowanalytics.com%252Fblog%252F2012%252F10%252F31%252Fsnowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow%252F%26ei%3DuQ9TUonxBcLL0QXc74DoDg%26usg%3DAFQjCNFWhV4rr2zmRm1fe4hNiay6Td9VrA%26bvm%3Dbv.53537100%2Cd.d2k&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1440x900&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2012%2F10%2F31%2Fsnowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow%2F&cv=clj-0.5.0-tom-0.0.4&nuid=8712a379-4bcb-46ee-815d-85f26540577f	-	-	-"
    )

  val expected = List(
    "snowplowweb",
    "web",
    "2013-10-07 19:47:54.000",
    "2013-10-07 19:47:54.123",
    "page_view",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "958446",
    null, // No tracker namespace
    "js-0.12.0",
    "clj-0.5.0-tom-0.0.4",
    EtlVersion,
    null, // No user_id set
    "255.255.x.x",
    "812263905",
    "d159c05f2aa8e1b9",
    "1",
    "8712a379-4bcb-46ee-815d-85f26540577f",
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    "http://snowplowanalytics.com/blog/2012/10/31/snowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow/",
    "Snowplow in a Universal Analytics world - what the new version of Google Analytics means for companies adopting Snowplow - Snowplow Analytics",
    "https://www.google.co.uk/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CDsQFjAC&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2012%2F10%2F31%2Fsnowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow%2F&ei=uQ9TUonxBcLL0QXc74DoDg&usg=AFQjCNFWhV4rr2zmRm1fe4hNiay6Td9VrA&bvm=bv.53537100,d.d2k",
    "http",
    "snowplowanalytics.com",
    "80",
    "/blog/2012/10/31/snowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow/",
    null,
    null,
    "https",
    "www.google.co.uk",
    "80",
    "/url",
    "sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CDsQFjAC&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2012%2F10%2F31%2Fsnowplow-in-a-universal-analytics-world-what-the-new-version-of-google-analytics-means-for-companies-adopting-snowplow%2F&ei=uQ9TUonxBcLL0QXc74DoDg&usg=AFQjCNFWhV4rr2zmRm1fe4hNiay6Td9VrA&bvm=bv.53537100,d.d2k",
    null,
    "search", // Search referer
    "Google",
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
    null, // Unstructured event fields empty
    null, //
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36",
    "Chrome",
    "Chrome",
    null,
    "Browser",
    "WEBKIT",
    "en-US",
    "1",
    "1",
    "1",
    "0",
    "1",
    "0",
    "0",
    "0",
    "1",
    "1",
    "24",
    "1440",
    "802",
    "Mac OS",
    "Mac OS",
    "Apple Inc.",
    "Europe/London",
    "Computer",
    "0",
    "1440",
    "900",
    "UTF-8",
    "1425",
    "4674"
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page view in the
 * CloudFront format changed in August 2013
 * are successfully extracted.
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class CljTomcatLineSpec extends Specification with TupleConversions {

  "A job which processes a Clojure-Tomcat file containing 1 valid page view" should {
    EtlJobSpec("clj-tomcat", "2").
      source(MultipleTextLineFiles("inputFolder"), CljTomcatLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CljTomcatLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CljTomcatLineSpec.expected(idx), withIndex = idx)
          }
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}