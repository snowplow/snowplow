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
package loaders

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary._

class CloudfrontLoaderSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the CloudfrontLoader functionality"                                        ^
                                                                                                             p^
  "toTimestamp should create a DateTime from valid date and time Strings"                                     ! e1^
  "toOption should return a None if the querystring is empty"                                                 ! e2^
  "toCleanUri should remove a trailing % from a URI correctly"                                                ! e3^
  "singleEncodePcts should correctly single-encoding double-encoded % signs"                                  ! e4^
  "toCollectorPayload should return a CanonicalInput for a valid CloudFront log record"                       ! e5^
  "toCollectorPayload should return a Validation Failure for a non-GET request to /i"                         ! e6^
  "toCollectorPayload should return a Validation Failure for an invalid or corrupted CloudFront log record"   ! e7^
                                                                                                            end

  object Expected {
    val collector = "cloudfront"
    val encoding  = "UTF-8"
    val api       = CollectorApi("com.snowplowanalytics.snowplow", "tp1")
  }

  def e1 = 
    "SPEC NAME"           || "DATE"       | "TIME"         | "EXP. DATETIME"                                 |
    "Valid with ms #1"    !! "2003-12-04" ! "00:18:48.234" ! DateTime.parse("2003-12-04T00:18:48.234+00:00") |
    "Valid with ms #2"    !! "2011-08-29" ! "23:56:01.003" ! DateTime.parse("2011-08-29T23:56:01.003+00:00") |
    "Valid without ms #1" !! "2013-05-12" ! "17:34:10"     ! DateTime.parse("2013-05-12T17:34:10+00:00")     |
    "Valid without ms #2" !! "1980-04-01" ! "21:20:04"     ! DateTime.parse("1980-04-01T21:20:04+00:00")     |> {
      (_, date, time, expected) => {
        val actual = CloudfrontLoader.toTimestamp(date, time)     
        actual must beSuccessful(expected)
      }
    }

  def e2 =
    foreach(Seq(null, "", "-")) { empty: String =>
        CloudfrontLoader.toOption(empty) must beNone
      }

  def e3 =
    "SPEC NAME"                 || "URI"                                              | "EXP. URI"                                        |
    "URI with trailing % #1"    !! "https://github.com/snowplow/snowplow/issues/494%" ! "https://github.com/snowplow/snowplow/issues/494" |
    "URI with trailing % #2"    !! "http://bbc.co.uk%"                                ! "http://bbc.co.uk"                                |
    "URI without trailing % #1" !! "https://github.com/snowplow/snowplow/issues/494"  ! "https://github.com/snowplow/snowplow/issues/494" |
    "URI without trailing % #2" !! "http://bbc.co.uk"                                 ! "http://bbc.co.uk"                                |> {
      (_, uri, expected) => {
        val actual = CloudfrontLoader.toCleanUri(uri)     
        actual  must_== expected
      }
    }

  def e4 =
    "SPEC NAME"                                 || "QUERYSTRING"                                    | "EXP. QUERYSTRING"                             |
    "Double-encoded %s, modify"                 !! "e=pv&page=Celestial%2520Tarot%2520-%2520Psychic%2520Bazaar&dtm=1376487150616&tid=483686&vp=1097x482&ds=1097x1973&vid=1&duid=1f2719e9217b5e1b&p=web&tv=js-0.12.0&fp=3748874661&aid=pbzsite&lang=en-IE&cs=utf-8&tz=Europe%252FLondon&refr=http%253A%252F%252Fwww.psychicbazaar.com%252Fsearch%253Fsearch_query%253Dcelestial%252Btarot%252Bdeck&f_java=1&res=1097x617&cd=24&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F48-celestial-tarot.html" ! "e=pv&page=Celestial%20Tarot%20-%20Psychic%20Bazaar&dtm=1376487150616&tid=483686&vp=1097x482&ds=1097x1973&vid=1&duid=1f2719e9217b5e1b&p=web&tv=js-0.12.0&fp=3748874661&aid=pbzsite&lang=en-IE&cs=utf-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2Fsearch%3Fsearch_query%3Dcelestial%2Btarot%2Bdeck&f_java=1&res=1097x617&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F48-celestial-tarot.html" |
    "Ambiguous - assume double-encoded, modify" !! "%2588 is 1x-encoded 25 percent OR 2x-encoded ^" ! "%88 is 1x-encoded 25 percent OR 2x-encoded ^" |
    "Single-encoded %s, leave"                  !! "e=pp&page=Dreaming%20Way%20Tarot%20-%20Psychic%20Bazaar&pp_mix=0&pp_max=0&pp_miy=0&pp_may=0&dtm=1376984181667&tid=056188&vp=1440x838&ds=1440x1401&vid=1&duid=8ac2d67163d6d36a&p=web&tv=js-0.12.0&fp=1569742263&aid=pbzsite&lang=en-us&cs=UTF-8&tz=Australia%2FSydney&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1440x900&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F312-dreaming-way-tarot.html" ! "e=pp&page=Dreaming%20Way%20Tarot%20-%20Psychic%20Bazaar&pp_mix=0&pp_max=0&pp_miy=0&pp_may=0&dtm=1376984181667&tid=056188&vp=1440x838&ds=1440x1401&vid=1&duid=8ac2d67163d6d36a&p=web&tv=js-0.12.0&fp=1569742263&aid=pbzsite&lang=en-us&cs=UTF-8&tz=Australia%2FSydney&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1440x900&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F312-dreaming-way-tarot.html" |
    "Single-encoded % sign itself, leave"       !! "Loading - 70%25 Complete"                       ! "Loading - 70%25 Complete"                     |> {
      (_, qs, expected) => {
        val actual = ConversionUtils.singleEncodePcts(qs)
        actual must_== expected
      }
    }

  def e5 =
    "SPEC NAME"                || "RAW" | "EXP. TIMESTAMP"                                | "EXP. PAYLOAD" | "EXP. IP ADDRESS"      | "EXP. USER AGENT"                                                                    | "EXP. REFERER URI"                                       |
    "CloudFront with 2 spaces" !! "2013-08-29  00:18:48  LAX3  830 255.255.255.255 GET d3v6ndkyapxc2w.cloudfront.net /i  200 http://snowplowanalytics.com/analytics/index.html Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0 e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html - Hit wQ1OBZtQlGgfM_tPEJ-lIQLsdra0U-lXgmfJfwja2KAV_SfTdT3lZg==" !
                                          DateTime.parse("2013-08-29T00:18:48.000+00:00") ! toNameValuePairs("e" -> "pv", "page" -> "Introduction - Snowplow Analytics%", "dtm" -> "1377735557970", "tid" -> "567074", "vp" -> "1024x635", "ds" -> "1024x635", "vid" -> "1", "duid" -> "7969620089de36eb", "p" -> "web", "tv" -> "js-0.12.0", "fp" -> "308909339", "aid" -> "snowplowweb", "lang" -> "en-US", "cs" -> "UTF-8", "tz" -> "America/Los_Angeles", "refr" -> "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=", "f_pdf" -> "1", "f_qt" -> "1", "f_realp" -> "0", "f_wma" -> "1", "f_dir" -> "0", "f_fla" -> "1", "f_java" -> "1", "f_gears" -> "0", "f_ag" -> "0", "res" -> "1024x768", "cd" -> "24", "cookie" -> "1", "url" -> "http://snowplowanalytics.com/analytics/index.html") !
                                                                                                             "255.255.255.255".some ! "Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0".some ! "http://snowplowanalytics.com/analytics/index.html".some |
    "CloudFront with 4 spaces" !! "2014-01-28     02:52:24     HKG50     829     202.134.75.113     GET     d3v6ndkyapxc2w.cloudfront.net     /i     200     http://snowplowanalytics.com/product/index.html     Mozilla/5.0%2520(Windows%2520NT%25205.1)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/31.0.1650.57%2520Safari/537.36     e=pv&page=Snowplow%2520-%2520the%2520most%2520powerful%252C%2520scalable%252C%2520flexible%2520web%2520analytics%2520platform%2520in%2520the%2520world.%2520-%2520Snowplow%2520Analytics&tid=322602&vp=1600x739&ds=1600x739&vid=1&duid=5c34698b211e8949&p=web&tv=js-0.13.0&aid=snowplowweb&lang=zh-CN&cs=UTF-8&tz=Asia%252FShanghai&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fabout%252Findex.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1600x900&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fproduct%252Findex.html     -     Hit     VtgzUTq1UoySDN3m_B-5DqmpTjgAS5YaAcvk_uz_D0-0TrDrZJJu2Q==     d3v6ndkyapxc2w.cloudfront.net     http     881" !
                                          DateTime.parse("2014-01-28T02:52:24.000+00:00") ! toNameValuePairs("e" -> "pv", "page" -> "Snowplow - the most powerful, scalable, flexible web analytics platform in the world. - Snowplow Analytics", "tid" -> "322602", "vp" -> "1600x739", "ds" -> "1600x739", "vid" -> "1", "duid" -> "5c34698b211e8949", "p" -> "web", "tv" -> "js-0.13.0", "aid" -> "snowplowweb", "lang" -> "zh-CN", "cs" -> "UTF-8", "tz" -> "Asia/Shanghai", "refr" -> "http://snowplowanalytics.com/about/index.html", "f_pdf" -> "1", "f_qt" -> "1", "f_realp" -> "0", "f_wma" -> "1", "f_dir" -> "0", "f_fla" -> "1", "f_java" -> "1", "f_gears" -> "0", "f_ag" -> "1", "res" -> "1600x900", "cookie" -> "1", "url" -> "http://snowplowanalytics.com/product/index.html") !
                                                                                                             "202.134.75.113".some ! "Mozilla/5.0%20(Windows%20NT%205.1)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/31.0.1650.57%20Safari/537.36".some ! "http://snowplowanalytics.com/product/index.html".some |
    "CloudFront with tabs"     !! "2014-01-28	03:41:59	IAD12	828	67.71.16.237	GET	d10wr4jwvp55f9.cloudfront.net	/i	200	http://www.psychicbazaar.com/oracles/107-magdalene-oracle.html	Mozilla/5.0%2520(Windows%2520NT%25206.1;%2520Trident/7.0;%2520rv:11.0)%2520like%2520Gecko	e=pp&page=Magdalene%2520Oracle%2520-%2520Psychic%2520Bazaar&tid=151507&vp=975x460&ds=1063x1760&vid=1&duid=44a32544aac965f4&p=web&tv=js-0.13.0&aid=pbzsite&lang=en-CA&cs=utf-8&tz=America%252FHavana&refr=http%253A%252F%252Fwww.google.ca%252Furl%253Fsa%253Dt%2526rct%253Dj%2526q%253D%2526esrc%253Ds%2526source%253Dweb%2526cd%253D16%2526ved%253D0CIIBEBYwDw%2526url%253Dhttp%25253A%25252F%25252Fwww.psychicbazaar.com%25252Foracles%25252F107-magdalene-oracle.html%2526ei%253DIibnUsfBDMiM2gXGoICoDg%2526usg%253DAFQjCNE6fEqO8lnxDHeke0LOuAZIa1iSFQ%2526sig2%253DV7KJR0VmGw5yaHoMKKJHhg%2526bvm%253Dbv.59930103%252Cd.b2I&f_pdf=0&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=0&f_java=1&f_gears=0&f_ag=1&res=975x571&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Foracles%252F107-magdalene-oracle.html	-	Hit	7T7tuHtEcdoDvUuGnQ3F0RI_UEWOUeb0b-YIhcoxjziuEBMDcKv_OA==	d10wr4jwvp55f9.cloudfront.net	http	1047" !
                                          DateTime.parse("2014-01-28T03:41:59.000+00:00") ! toNameValuePairs("e" -> "pp", "page" -> "Magdalene Oracle - Psychic Bazaar", "tid" -> "151507", "vp" -> "975x460", "ds" -> "1063x1760", "vid" -> "1", "duid" -> "44a32544aac965f4", "p" -> "web", "tv" -> "js-0.13.0", "aid" -> "pbzsite", "lang" -> "en-CA", "cs" -> "utf-8", "tz" -> "America/Havana", "refr" -> "http://www.google.ca/url?sa=t&rct=j&q=&esrc=s&source=web&cd=16&ved=0CIIBEBYwDw&url=http%3A%2F%2Fwww.psychicbazaar.com%2Foracles%2F107-magdalene-oracle.html&ei=IibnUsfBDMiM2gXGoICoDg&usg=AFQjCNE6fEqO8lnxDHeke0LOuAZIa1iSFQ&sig2=V7KJR0VmGw5yaHoMKKJHhg&bvm=bv.59930103,d.b2I", "f_pdf" -> "0", "f_qt" -> "0", "f_realp" -> "0", "f_wma" -> "0", "f_dir" -> "0", "f_fla" -> "0", "f_java" -> "1", "f_gears" -> "0", "f_ag" -> "1", "res" -> "975x571", "cookie" -> "1", "url" -> "http://www.psychicbazaar.com/oracles/107-magdalene-oracle.html") !
                                                                                                             "67.71.16.237".some ! "Mozilla/5.0%20(Windows%20NT%206.1;%20Trident/7.0;%20rv:11.0)%20like%20Gecko".some ! "http://www.psychicbazaar.com/oracles/107-magdalene-oracle.html".some |> {

      (_, raw, timestamp, payload, ipAddress, userAgent, refererUri) => {

        val canonicalEvent = CloudfrontLoader
          .toCollectorPayload(raw)

        val expected = CollectorPayload(
          api          = Expected.api,
          querystring  = payload,
          body         = None,
          contentType  = None,
          source       = CollectorSource(Expected.collector, Expected.encoding, None),
          context      = CollectorContext(timestamp.some, ipAddress, userAgent, refererUri, Nil, None)
          )
    
        canonicalEvent must beSuccessful(expected.some)
      }
    }

  def e6 = {
    val raw = "2012-05-24  11:35:53  DFW3  3343  99.116.172.58 POST d3gs014xn8p70.cloudfront.net  /i  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  e=pv&page=Tarot%2520cards%2520-%2520Psychic%2520Bazaar&tid=344260&uid=288112e0a5003be2&vid=1&lang=en-US&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fp%253D4&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1366x768&cookie=1"
    CloudfrontLoader.toCollectorPayload(raw) must beFailing(NonEmptyList("Only GET operations supported for CloudFront Collector, not POST"))
  }

  // A bit of fun: the chances of generating a valid CloudFront row at random are
  // so low that we can just use ScalaCheck here
  def e7 =
    check { (raw: String) => CloudfrontLoader.toCollectorPayload(raw) must beFailing(NonEmptyList("Line does not match CloudFront header or data row formats")) }
}
