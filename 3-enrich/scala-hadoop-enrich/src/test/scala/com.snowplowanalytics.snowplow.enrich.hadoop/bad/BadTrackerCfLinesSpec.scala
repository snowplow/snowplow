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
package bad

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
object BadTrackerCfLinesSpec {

  val lines = Lines(
    "2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  e=pv&p=mobile&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1",
    "2012-05-24  00:06:42  LHR5  3402  213.52.50.8  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  e=lol&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=a&p=web&aid=CFe23a&fp=1906624389&tz=Europe%2FLondon&cd=24&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1",
    "2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  e=lol&ue_px=am9obitzbWl0aA&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=a&p=web&aid=CFe23a&fp=1906624389&tz=Europe%2FLondon&cd=24&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1"
    )

  val expected = List(
    """{"line":"2012-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  e=pv&p=mobile&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=721410&uid=3798cdce0493133e&vid=1&lang=en&refr=http%253A%252F%252Fwww.google.com%252Fm%252Fsearch&res=640x960&cookie=1","errors":["Field [p]: [mobile] is not a supported tracking platform"]}""",
    """{"line":"2012-05-24  00:06:42  LHR5  3402  213.52.50.8  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  e=lol&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=a&p=web&aid=CFe23a&fp=1906624389&tz=Europe%2FLondon&cd=24&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1","errors":["Field [e]: [lol] is not a recognised event code","Field [vid]: cannot convert [a] to Int"]}""",
    """{"line":"2012-05-24  00:06:42  LHR5  3402  90.194.12.51  GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3  e=lol&ue_px=am9obitzbWl0aA&page=Psycards%2520book%2520and%2520deck%2520starter%2520pack%2520-%2520Psychic%2520Bazaar&tid=019539&uid=e7bccbb647296c98&vid=a&p=web&aid=CFe23a&fp=1906624389&tz=Europe%2FLondon&cd=24&lang=en-us&refr=http%253A%252F%252Fwww.google.com%252Fsearch%253Fhl%253Den%2526q%253Dthe%252Bpsycard%252Bstory%2526oq%253Dthe%252Bpsycard%252Bstory%2526aq%253Df%2526aqi%253D%2526aql%253D%2526gs_l%253Dmobile-gws-serp.12...0.0.0.6358.0.0.0.0.0.0.0.0..0.0...0.0.JrNbKlRgHbQ%2526mvs%253D0&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=1&f_ag=0&res=320x480&cookie=1","errors":["[lol] is not a recognised event code","Field [vid]: cannot convert [a] to Int","Field [ue_px]: invalid JSON with parsing error: Unexpected content found: john+smith"]}"""
    )
}

/**
 * Integration test for the EtlJob:
 *
 * CloudFront-format rows which contain "bad data"
 * from the tracker.
 */
class BadTrackerCfLinesSpec extends Specification with TupleConversions {

  "A job which processes input lines containing corrupted data from the tracker" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), BadTrackerCfLinesSpec.lines).
      sink[String](Tsv("outputFolder")){ output => 
        "not write any events" in {
          output must beEmpty
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ buf =>
        "write bad row JSONs, each containing an input line and the errors" in {
          buf(0) must_== BadTrackerCfLinesSpec.expected(0)
          buf(1) must_== BadTrackerCfLinesSpec.expected(1)
        }
      }.
      run.
      finish
  }
}