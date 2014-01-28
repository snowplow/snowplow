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
package com.snowplowanalytics.snowplow.enrich.common
package inputs

// Joda-Time
import org.joda.time.DateTime

// Snowplow
import LoaderSpecHelpers._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

object CloudfrontLikeLoaderSpec {

  val cfLikeLoader = new CloudfrontLikeLoader {
    // Make our trait whole
    def getSource = InputSource("mock", None)
    def isActualCloudfront = false
  }  
}

class CloudfrontLikeLoaderSpec extends Specification with DataTables with ValidationMatchers {

  import CloudfrontLikeLoaderSpec._

  "toTimestamp" should {
    "create a DateTime from valid date and time Strings" in {

      "SPEC NAME"           || "DATE"       | "TIME"         | "EXP. DATETIME"                                 |
      "Valid with ms #1"    !! "2003-12-04" ! "00:18:48.234" ! DateTime.parse("2003-12-04T00:18:48.234+00:00") |
      "Valid with ms #2"    !! "2011-08-29" ! "23:56:01.003" ! DateTime.parse("2011-08-29T23:56:01.003+00:00") |
      "Valid without ms #1" !! "2013-05-12" ! "17:34:10"     ! DateTime.parse("2013-05-12T17:34:10+00:00")     |
      "Valid without ms #2" !! "1980-04-01" ! "21:20:04"     ! DateTime.parse("1980-04-01T21:20:04+00:00")     |> {
        (_, date, time, expected) => {
          val actual = cfLikeLoader.toTimestamp(date, time)     
          actual must beSuccessful(expected)
        }
      }

    }
  }

  "toOption" should {
    "return a None if the querystring is empty" in {
      foreach(Seq(null, "", "-")) { empty: String =>
        cfLikeLoader.toOption(empty) must beNone
      }
    }
  }

  "toCleanUri" should {
    "remove a trailing % from a URI correctly" in {

      "SPEC NAME"                 || "URI"                                              | "EXP. URI"                                        |
      "URI with trailing % #1"    !! "https://github.com/snowplow/snowplow/issues/494%" ! "https://github.com/snowplow/snowplow/issues/494" |
      "URI with trailing % #2"    !! "http://bbc.co.uk%"                                ! "http://bbc.co.uk"                                |
      "URI without trailing % #1" !! "https://github.com/snowplow/snowplow/issues/494"  ! "https://github.com/snowplow/snowplow/issues/494" |
      "URI without trailing % #2" !! "http://bbc.co.uk"                                 ! "http://bbc.co.uk"                                |> {
        (_, uri, expected) => {
          val actual = cfLikeLoader.toCleanUri(uri)     
          actual  must_== expected
        }
      }
    }
  }

  "singleEncodePcts" should {
    "correctly single-encoding double-encoded % signs" in {

      "SPEC NAME"                                 || "QUERYSTRING"                                    | "EXP. QUERYSTRING"                               |
      "Double-encoded %s, modify"                 !! "e=pv&page=Celestial%2520Tarot%2520-%2520Psychic%2520Bazaar&dtm=1376487150616&tid=483686&vp=1097x482&ds=1097x1973&vid=1&duid=1f2719e9217b5e1b&p=web&tv=js-0.12.0&fp=3748874661&aid=pbzsite&lang=en-IE&cs=utf-8&tz=Europe%252FLondon&refr=http%253A%252F%252Fwww.psychicbazaar.com%252Fsearch%253Fsearch_query%253Dcelestial%252Btarot%252Bdeck&f_java=1&res=1097x617&cd=24&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F48-celestial-tarot.html" ! "e=pv&page=Celestial%20Tarot%20-%20Psychic%20Bazaar&dtm=1376487150616&tid=483686&vp=1097x482&ds=1097x1973&vid=1&duid=1f2719e9217b5e1b&p=web&tv=js-0.12.0&fp=3748874661&aid=pbzsite&lang=en-IE&cs=utf-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2Fsearch%3Fsearch_query%3Dcelestial%2Btarot%2Bdeck&f_java=1&res=1097x617&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F48-celestial-tarot.html" |
      "Ambiguous - assume double-encoded, modify" !! "%2588 is 1x-encoded 25 percent OR 2x-encoded ^" ! "%88 is 1x-encoded 25 percent OR 2x-encoded ^" |
      "Single-encoded %s, leave"                  !! "e=pp&page=Dreaming%20Way%20Tarot%20-%20Psychic%20Bazaar&pp_mix=0&pp_max=0&pp_miy=0&pp_may=0&dtm=1376984181667&tid=056188&vp=1440x838&ds=1440x1401&vid=1&duid=8ac2d67163d6d36a&p=web&tv=js-0.12.0&fp=1569742263&aid=pbzsite&lang=en-us&cs=UTF-8&tz=Australia%2FSydney&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1440x900&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F312-dreaming-way-tarot.html" ! "e=pp&page=Dreaming%20Way%20Tarot%20-%20Psychic%20Bazaar&pp_mix=0&pp_max=0&pp_miy=0&pp_may=0&dtm=1376984181667&tid=056188&vp=1440x838&ds=1440x1401&vid=1&duid=8ac2d67163d6d36a&p=web&tv=js-0.12.0&fp=1569742263&aid=pbzsite&lang=en-us&cs=UTF-8&tz=Australia%2FSydney&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1440x900&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F312-dreaming-way-tarot.html" |
      "Single-encoded % sign itself, leave"       !! "Loading - 70%25 Complete"                       ! "Loading - 70%25 Complete"                     |> {
        (_, qs, expected) => {
          val actual = cfLikeLoader.singleEncodePcts(qs)     
          actual must_== expected
        }
      }
    }
  }

}
