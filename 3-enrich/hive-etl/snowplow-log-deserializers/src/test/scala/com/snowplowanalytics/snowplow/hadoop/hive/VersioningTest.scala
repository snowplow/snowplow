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
package com.snowplowanalytics.snowplow.hadoop.hive

// Specs2
import org.specs2.mutable.Specification

// Deserializer
import test.SnowPlowDeserializer

class VersioningTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Row which contains tabs
  val row = "2012-06-09	00:12:34	EWR2	3345	184.151.127.250	GET	d3gs014xn8p70.cloudfront.net	/ice.png	200	http://www.psychicbazaar.com/publisher/8_piatnik	Mozilla/5.0%20(Macintosh;%20U;%20Intel%20Mac%20OS%20X%2010_6_2;%20en-us)%20AppleWebKit/531.21.8%20(KHTML,%20like%20Gecko)%20Version/4.0.4%20Safari/531.21.10	page=%250APublisher%253A%2520Piatnik%2520-%2520Psychic%2520Bazaar&tid=397770&duid=825c94ab288ad859&vid=5&lang=en-us&refr=http%253A%252F%252Fwww.google.ca%252Fimgres%253Fimgurl%253Dhttp%253A%252F%252Fmdm.pbzstatic.com%252Ftarot%252Fpetrak-tarot%252Fmontage.png%2526imgrefurl%253Dhttp%253A%252F%252Fwww.psychicbazaar.com%252Fpublisher%252F8_piatnik%2526usg%253D__SuKRFvoIHha8fX6oh_-k3Rt3EdQ%253D%2526h%253D250%2526w%253D734%2526sz%253D241%2526hl%253Den%2526start%253D11%2526zoom%253D1%2526tbnid%253DcRZVciqXz-3AHM%253A%2526tbnh%253D48%2526tbnw%253D141%2526ei%253D2JTST-bYKMa-0QHTwZyEAw%2526prev%253D%252Fsearch%25253Fq%25253Dpetra%25252BK%25252Bpiatnik%25252Btarot%252526hl%25253Den%252526client%25253Dsafari%252526sa%25253DX%252526rls%25253Den%252526tbm%25253Disch%252526prmd%25253Divns%2526itbs%253D1&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=1&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cookie=1&url=http%253A%252F%252Fwww.psychicbazaar.com%252Fpublisher%252F8_piatnik&tv=js-0.11.0";
  
  "A SnowPlow row" should {

    val actual = SnowPlowDeserializer.deserialize(row)
  
    "have correct versioning information" in {
      actual.v_tracker must_== "js-0.11.0"
      actual.v_collector must_== "cf"
      actual.v_etl must_== "serde-0.5.5"
    }
  } 
}