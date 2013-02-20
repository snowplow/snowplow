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

class TabTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Row which contains tabs
  val badRow = "2012-05-25  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=5 Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  page=%09TEST%20PAGE%09&tid=662569&duid=30e808cb71d136f4&vid=1&aid=CFe23a&lang=en-GB&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=2560x1440&cookie=1&url=file%3A%2F%2F%2Fhome%2Falex%2FDevelopment%2FSnowPlow%2Fsnowplow%2F1-trackers%2Fjavascript-tracker%2Fexamples%2Fweb%2Fasync.html"
  
  "A SnowPlow row where fields contain tabs" should {

    val actual = SnowPlowDeserializer.deserialize(badRow)
  
    "change the tabs into four spaces" in {
      actual.page_title must_== "    TEST PAGE    "
    }
  } 
}