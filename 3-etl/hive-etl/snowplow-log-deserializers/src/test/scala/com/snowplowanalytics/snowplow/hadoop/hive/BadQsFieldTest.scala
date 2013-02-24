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

class BadQsFieldTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  // Contains a bad querystring field - "referer" not "refr"
  val badField = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&tid=236095&referer=http%253A%252F%252Ftest.psybazaar.com%252F&duid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"

  "A SnowPlow querystring with an incorrectly named field (\"referer\" not \"refr\")" should {
    "not return a <<null>> record" in {
      SnowPlowDeserializer.deserialize(badField).dt must not beNull
    }
  }

  // Contains an extra querystring field - "future"
  val extraField = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&tid=236095&future=1&refr=http%253A%252F%252Ftest.psybazaar.com%252F&duid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"

  "A SnowPlow querystring with an extra field (\"future\")" should {
    "not return a <<null>> record" in {
      SnowPlowDeserializer.deserialize(extraField).dt must not beNull
    }
  }

  // Contains an unescaped referer
  // Happens in case of tracker malfunction
  val unescapedField = "2012-12-10  03:05:09  LHR5  3703  207.189.121.44  GET d10wr4jwvp55f9.cloudfront.net /ice.png  200 - Mozilla/5.0%20(Windows;%20U;%20Windows%20NT%205.1;%20en-US;%20rv:1.9.2.8)%20Gecko/20100721%20Firefox/3.6.8  page=Publisher:%20Piatnik%20-%20Psychic%20Bazaar&tid=128078&duid=ea4bcf975f101eec&vid=2&lang=en-gb&refr=http://www.google.co.uk/search?hl=en&tbo=d&site=&source=hp&q=piatnik+tarot&oq=piatnik+tarot&gs_l=mobile-gws-hp.1.0.0j0i30l2j0i8i10i30j0i8i30.11955.26264.0.29732.17.14.2.1.1.0.1060.4823.2j2j5j0j2j1j1j1.14.0.les%253B..0.0...1ac.1.vP9Vltg2PPw&f_pdf=0&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=0&f_java=0&f_gears=0&f_ag=0&res=320x568&cookie=1&url=http://www.psychicbazaar.com/publisher/8_piatnik?utm_source=GoogleSearch&utm_medium=cpc&utm_campaign=uk-piatnik&utm_term=piatnik%2520tarot%2520cards&utm_content=29271604528&gclid=CL3159nCjLQCFe7MtAod83wACA  - RefreshHit  9rjpHm7OYpiNE7bURuP3cGbsem974NNrIoxgdQ6XuQm6Ils0d6_mUQ=="

  "A SnowPlow querystring with an unescaped field (\"refr\")" should {
    val actual = SnowPlowDeserializer.deserialize(unescapedField)
    "not return a <<null>> record" in {
      actual.dt must not beNull
    }
    "have a <<null>> event field" in {
      actual.event must beNull
    }
  }
}