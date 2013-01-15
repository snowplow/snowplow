/*
 * Copyright (c) 2012 Twitter, Inc.
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
package com.snowplowanalytics.snowplow.hadoop.etl.integrations

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

/**
 * Integration test for SnowPlowEtlJob:
 *
 * placeholder that needs updating.
 */
class SnowPlowEtlJobTest extends Specification with TupleConversions {

  "A WordCount job" should {
        "count words correctly" in {

    JobTest("com.snowplowanalytics.snowplow.hadoop.etl.EtlJob").
      arg("INPUT_FOLDER", "inputFolder").
      arg("INPUT_FORMAT", "cloudfront").
      arg("OUTPUT_FOLDER", "outputFolder").
      arg("ERRORS_FOLDER", "errorFolder").
      arg("CONTINUE_ON", "1").
      source(MultipleTextLineFiles("inputFolder"), List("0" -> "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  e=pv&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1")).
      sink[String](TextLine("outputFolder")){ buf =>

        val line = buf.head

          line must_== "OH NOES"
      }.
      sink[String](JsonLine("errorFolder")){ _ => Unit}.
      run.
      finish
      success
  }}
}

/**
 * Integration test for SnowPlowEtlJob:
 *
 * placeholder that needs updating.
 */
class SnowPlowEtlJobTest2 extends Specification with TupleConversions {

  "A WordCount job" should {
        "count words correctly" in {

    JobTest("com.snowplowanalytics.snowplow.hadoop.etl.EtlJob").
      arg("INPUT_FOLDER", "inputFolder").
      arg("INPUT_FORMAT", "cloudfront").
      arg("OUTPUT_FOLDER", "outputFolder").
      arg("ERRORS_FOLDER", "errorFolder").
      arg("CONTINUE_ON", "1").
      source(MultipleTextLineFiles("inputFolder"), List("0" -> "20yy-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  e=pv&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1")).
      sink[String](TextLine("outputFolder")){ _ => Unit }.
      sink[String](JsonLine("errorFolder")){ buf =>

        val line = buf.head

          line must_== "OH NOES"
      }.
      run.
      finish
      success
  }}
}