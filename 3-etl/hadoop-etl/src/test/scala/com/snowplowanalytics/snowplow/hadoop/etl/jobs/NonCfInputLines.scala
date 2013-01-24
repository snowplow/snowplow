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
package com.snowplowanalytics.snowplow.hadoop.etl
package jobs

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// This project
// TODO: remove this when Scalding 0.8.3 released
import utils.Json2Line
import TestHelpers._

/**
 * Integration test for the EtlJob:
 *
 * Input data _is_ not in the
 * expected CloudFront format.
 */
class NonCfInputLinesTest extends Specification with TupleConversions {

  "A job which processes an input line which is not CloudFront format" should {
    "write an error JSON containing the input line and the appropriate error message" in {

    	val badLines = List(
        "0" -> "",
        "1" -> "NOT VALID",
        "2" -> "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net")

      EtlJobTest.
        source(MultipleTextLineFiles("inputFolder"), badLines).
        sink[String](TextLine("outputFolder")){ _ => Unit }.
        sink[String](Json2Line("errorFolder")){ buf =>
          val json = buf.head
          json must_== """{"line":"20yy-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  -","errors":["Unexpected exception converting date [20yy-05-24] and time [00:08:40] to timestamp: [Invalid format: \"20yy-05-24T00:08:40\" is malformed at \"yy-05-24T00:08:40\"]","Querystring is empty, cannot extract GET payload"]}"""
        }.
        run.
        finish
        success
  }}
}