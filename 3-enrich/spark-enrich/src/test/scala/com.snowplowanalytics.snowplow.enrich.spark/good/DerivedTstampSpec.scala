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

object DerivedTstampSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2013-10-07 23:35:30    LHR3    828 70.46.123.145   GET d10wr4jwvp55f9.cloudfront.net   /i  200 http://www.psychicbazaar.com/2-tarot-cards/genre/native%2520american/type/all/view/list?utm_source=GoogleSearch&utm_medium=cpc&utm_term=native%2520american%2520tarot%2520deck&utm_content=39254295088&cid=uk-tarot--native-american&gclid=CI6thtbxhboCFTMctAod0FcAdQ  Mozilla/5.0%2520(compatible;%2520MSIE%25209.0;%2520Windows%2520NT%25206.0;%2520Trident/5.0) e=pp&page=Tarot%2520cards%2520-%2520Native%2520american%2520-%2520Psychic%2520Bazaar&pp_mix=0&pp_max=0&pp_miy=0&pp_may=0&stm=1381188927572&dtm=1381188927571&tid=734991&vp=784x532&ds=1063x1726&vid=1&sid=75d0fc90-cef4-11e4-8830-0800200c9a66&duid=81aa96d6d6ee6ad4&p=web&tv=js-0.12.0&fp=1202972880&aid=pbzsite&lang=en-gb&cs=utf-8&tz=Europe%252FLondon&refr=http%253A%252F%252Fwww.google.com%252Fuds%252Fafs%253Fq%253Dnative%252520american%252520tarot%252520deck%252520religion%252520and%252520spirituality%252520card%252520corn%252520dance%2525203%2526client%253Dmonstermarketplace-infosites-search%2526channel%253Doutboundteleservices%2526hl%253Den%2526adtest%253Dfalse%2526oe%253Dutf8%2526ie%253Dutf8%2526r%253Dm%2526adpage%253D1%2526fexp%253D21404%25252C7000108%2526jsei%253D4%2526format%253Dn3%25257Cn3%2526ad%253Dn6%2526nocache%253D8231381188870735%2526num%253D0%2526output%253Duds_ads_only%2526v%253D3%2526u_his%253D2%2526u_tz%253D60%2526dt%253D1381188870736%2526u_w%253D1024%2526u_h%253D768%2526biw%253D784%2526bih%253D515%2526psw%253D784%2526psh%253D269%2526frm%253D0%2526ui%253Duv3atlt20ld20lv20ff1st14sd12sv12sa10af3srslipva-wi562-wi562%2526rurl%253Dhttp%25253A%25252F%25252Fwww.vivasearch.com%25252Fsearch%25252F%25253Fq%25253Dnative%25252Bamerican%25252Btarot%25252Bdeck%25252Breligion%25252Band%25252Bspirituality%25252Bcard%25252Bcorn%25252Bdance%25252B3%252526t%25253DH476469%252526gc%25253Dw13%252526sid%25253DCNCRousx1br-1GbJoA3wpw%252526utm_source%25253DH%252526utm_medium%25253Dpaid%252526cid%25253DH476469%2526referer%253Dhttp%25253A%25252F%25252F14468.6103.1.gameshud.com%25252Ffp%25253Fip%25253D81.106.34.172%252526q%25253DNative%25252BAmerican%25252BTarot%25252BDeck%25252B%25252528Religion%25252Band%25252BSpirituality%25252529%25252B%2525255BCards%2525255D%25252Bcorn%25252Bdance%25252B3%252526ua%25253DMozilla%2525252F5.0%25252B%25252528compatible%2525253B%25252BMSIE%25252B9.0%2525253B%25252BWindows%25252BNT%25252B6.0%2525253B%25252BTrident%2525252F5.0%25252529%252526ts%25253D1381188845562%252526sig%25253D3f9mz-69KddNRBXDy8jc95VhqkjyWeTKvvhqTWa9tyDVTnCAEJ6B79LSwsgKcBLPtUfE62OQnCJuvbfJXoGdIS92QkObhKYW1G5yhSG7vCdt61Y6z7RcSupxJ2Y4bw3mzYbSBaegl5DlBva7KCE-JtnOCTrRa7e6hNMxUVr_gL1oBeYFqwy8JZx0VuXyaYisBjdRKQeAyHYf5cb3I6d1CmZXTN3J7h7X2bV1Vp2TkAuJRzcfAj-WOJtneQ&f_java=1&res=1024x768&cd=24&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fnative%252520american%252Ftype%252Fall%252Fview%252Flist%253Futm_source%253DGoogleSearch%2526legacy_campaign%253Dbad_campaign%2526utm_medium%253Dcpc%2526medium%253Dbad_campaign%2526utm_term%253Dnative%252520american%252520tarot%252520deck%2526utm_content%253D39254295088%2526source%253Dbad_source%2526medium%253Dbad_medium%2526legacy_term%253Dbad_term%2526cid%253Duk-tarot--native-american%2526gclid%253DCI6thtbxhboCFTMctAod0FcAdQ%2526_sp%253D4da6ee6d6d69aa18.1381188927100&ua=IE%25207%2520-%2520Mozilla%252F4.0%2520(compatible%253B%2520MSIE%25207.0%253B%2520Windows%2520NT%25205.1%253B%2520.NET%2520CLR%25201.1.4322%253B%2520.NET%2520CLR%25202.0.50727%253B%2520.NET%2520CLR%25203.0.04506.30)  -   Hit oNtMBkd1kwF3sDxbjn7s-uaTSS7xEELs7o7B7lPosUWuMy3lQLc_QA=="
  )

  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2013-10-07 23:35:30.000",
    "2013-10-07 23:35:27.571",
    "page_ping",
    null, // We can't predict the event_id
    "734991",
    null, // No tracker namespace
    "js-0.12.0",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "70.46.123.145",
    "1202972880",
    "81aa96d6d6ee6ad4",
    "1",
    null, // No network_userid set
    "US", // US geolocation
    "FL",
    "Delray Beach",
    null,
    "26.461502",
    "-80.0728",
    "Florida",
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    "http://www.psychicbazaar.com/2-tarot-cards/genre/native%20american/type/all/view/list?utm_source=GoogleSearch&legacy_campaign=bad_campaign&utm_medium=cpc&medium=bad_campaign&utm_term=native%20american%20tarot%20deck&utm_content=39254295088&source=bad_source&medium=bad_medium&legacy_term=bad_term&cid=uk-tarot--native-american&gclid=CI6thtbxhboCFTMctAod0FcAdQ&_sp=4da6ee6d6d69aa18.1381188927100",
    "Tarot cards - Native american - Psychic Bazaar",
    "http://www.google.com/uds/afs?q=native%20american%20tarot%20deck%20religion%20and%20spirituality%20card%20corn%20dance%203&client=monstermarketplace-infosites-search&channel=outboundteleservices&hl=en&adtest=false&oe=utf8&ie=utf8&r=m&adpage=1&fexp=21404%2C7000108&jsei=4&format=n3%7Cn3&ad=n6&nocache=8231381188870735&num=0&output=uds_ads_only&v=3&u_his=2&u_tz=60&dt=1381188870736&u_w=1024&u_h=768&biw=784&bih=515&psw=784&psh=269&frm=0&ui=uv3atlt20ld20lv20ff1st14sd12sv12sa10af3srslipva-wi562-wi562&rurl=http%3A%2F%2Fwww.vivasearch.com%2Fsearch%2F%3Fq%3Dnative%2Bamerican%2Btarot%2Bdeck%2Breligion%2Band%2Bspirituality%2Bcard%2Bcorn%2Bdance%2B3%26t%3DH476469%26gc%3Dw13%26sid%3DCNCRousx1br-1GbJoA3wpw%26utm_source%3DH%26utm_medium%3Dpaid%26cid%3DH476469&referer=http%3A%2F%2F14468.6103.1.gameshud.com%2Ffp%3Fip%3D81.106.34.172%26q%3DNative%2BAmerican%2BTarot%2BDeck%2B%2528Religion%2Band%2BSpirituality%2529%2B%255BCards%255D%2Bcorn%2Bdance%2B3%26ua%3DMozilla%252F5.0%2B%2528compatible%253B%2BMSIE%2B9.0%253B%2BWindows%2BNT%2B6.0%253B%2BTrident%252F5.0%2529%26ts%3D1381188845562%26sig%3D3f9mz-69KddNRBXDy8jc95VhqkjyWeTKvvhqTWa9tyDVTnCAEJ6B79LSwsgKcBLPtUfE62OQnCJuvbfJXoGdIS92QkObhKYW1G5yhSG7vCdt61Y6z7RcSupxJ2Y4bw3mzYbSBaegl5DlBva7KCE-JtnOCTrRa7e6hNMxUVr_gL1oBeYFqwy8JZx0VuXyaYisBjdRKQeAyHYf5cb3I6d1CmZXTN3J7h7X2bV1Vp2TkAuJRzcfAj-WOJtneQ",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards/genre/native%20american/type/all/view/list",
    "utm_source=GoogleSearch&legacy_campaign=bad_campaign&utm_medium=cpc&medium=bad_campaign&utm_term=native%20american%20tarot%20deck&utm_content=39254295088&source=bad_source&medium=bad_medium&legacy_term=bad_term&cid=uk-tarot--native-american&gclid=CI6thtbxhboCFTMctAod0FcAdQ&_sp=4da6ee6d6d69aa18.1381188927100",
    null,
    "http",
    "www.google.com",
    "80",
    "/uds/afs",
    "q=native%20american%20tarot%20deck%20religion%20and%20spirituality%20card%20corn%20dance%203&client=monstermarketplace-infosites-search&channel=outboundteleservices&hl=en&adtest=false&oe=utf8&ie=utf8&r=m&adpage=1&fexp=21404%2C7000108&jsei=4&format=n3%7Cn3&ad=n6&nocache=8231381188870735&num=0&output=uds_ads_only&v=3&u_his=2&u_tz=60&dt=1381188870736&u_w=1024&u_h=768&biw=784&bih=515&psw=784&psh=269&frm=0&ui=uv3atlt20ld20lv20ff1st14sd12sv12sa10af3srslipva-wi562-wi562&rurl=http%3A%2F%2Fwww.vivasearch.com%2Fsearch%2F%3Fq%3Dnative%2Bamerican%2Btarot%2Bdeck%2Breligion%2Band%2Bspirituality%2Bcard%2Bcorn%2Bdance%2B3%26t%3DH476469%26gc%3Dw13%26sid%3DCNCRousx1br-1GbJoA3wpw%26utm_source%3DH%26utm_medium%3Dpaid%26cid%3DH476469&referer=http%3A%2F%2F14468.6103.1.gameshud.com%2Ffp%3Fip%3D81.106.34.172%26q%3DNative%2BAmerican%2BTarot%2BDeck%2B%2528Religion%2Band%2BSpirituality%2529%2B%255BCards%255D%2Bcorn%2Bdance%2B3%26ua%3DMozilla%252F5.0%2B%2528compatible%253B%2BMSIE%2B9.0%253B%2BWindows%2BNT%2B6.0%253B%2BTrident%252F5.0%2529%26ts%3D1381188845562%26sig%3D3f9mz-69KddNRBXDy8jc95VhqkjyWeTKvvhqTWa9tyDVTnCAEJ6B79LSwsgKcBLPtUfE62OQnCJuvbfJXoGdIS92QkObhKYW1G5yhSG7vCdt61Y6z7RcSupxJ2Y4bw3mzYbSBaegl5DlBva7KCE-JtnOCTrRa7e6hNMxUVr_gL1oBeYFqwy8JZx0VuXyaYisBjdRKQeAyHYf5cb3I6d1CmZXTN3J7h7X2bV1Vp2TkAuJRzcfAj-WOJtneQ",
    null,
    "search", // Search referer
    "Google",
    "native american tarot deck religion and spirituality card corn dance 3",
    "cpc",
    "GoogleSearch",
    "native american tarot deck",
    "39254295088",
    "uk-tarot--native-american",
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
    "0",
    "0",
    "0",
    "0",
    "IE 7 - Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30)", // Note this was overridden by &ua=
    "Internet Explorer 7",
    "Internet Explorer",
    "7.0",
    "Browser",
    "TRIDENT",
    "en-gb",
    null, // IE cannot report browser features
    null,
    "1", // Java
    null,
    null,
    null,
    null,
    null,
    null,
    "1",
    "24",
    "784",
    "532",
    "Windows XP",
    "Windows",
    "Microsoft Corporation",
    "Europe/London",
    "Computer",
    "0",
    "1024",
    "768",
    "utf-8",
    "1063",
    "1726",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    "America/New_York",
    "CI6thtbxhboCFTMctAod0FcAdQ",
    "Google",
    null,
    "2013-10-07 23:35:27.572",
    "4da6ee6d6d69aa18",
    "2013-10-07 23:35:27.100",
    """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"IE","useragentMajor":"7","useragentMinor":"0","useragentPatch":null,"useragentVersion":"IE 7.0","osFamily":"Windows XP","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Windows XP","deviceFamily":"Other"}},{"schema":"iglu:com.acme/app_id/jsonschema/1-0-0","data":{"appIdUpper":"PBZSITE"}}]}""",
    "75d0fc90-cef4-11e4-8830-0800200c9a66",
    "2013-10-07 23:35:29.999"
  )
}

/**
 * Check the new fields were added correctly
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class DerivedTstampSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "derive-tstamp"
  sequential
  "A job which processes a CloudFront file containing 1 valid page ping with campaign attribution" +
  " fields" should {
    runEnrichJob(DerivedTstampSpec.lines, "cloudfront", "1", false, List("geo"), false, true)

    "correctly output 1 page ping" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- DerivedTstampSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(DerivedTstampSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
