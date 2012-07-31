/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

class ResFieldTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // Template row to hold a bad res (screen resolution) field
  val rowWithRes: (String => String) = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&uid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=" + _ + "&cookie=1"

  "An invalid screen resolution querystring field should throw an exception" >> {
    Seq("", "800x", "x600", "420Ax800") foreach { res =>
      "invalid screen resolution \"%s\" returns a <<null>> record".format(res) >> {
        SnowPlowEventDeserializer.deserializeLine(rowWithRes(res), DEBUG).asInstanceOf[SnowPlowEventStruct].dt must beNull
      }.pendingUntilFixed // This is checking the wrong row somehow
    }
  }
}