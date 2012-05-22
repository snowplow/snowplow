/*
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hive.serde

// Specs2
import org.specs2.mutable.Specification

class DeserializeTest extends Specification {

  // -------------------------------------------------------------------------------------------------------------------
  // Type checks
  // -------------------------------------------------------------------------------------------------------------------

  val line1 = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&r=236095&urlref=http%253A%252F%252Ftest.psybazaar.com%252F&_id=135f6b7536aff045&lang=en-US&visit=5&pdf=0&qt=1&realp=0&wma=1&dir=0&fla=1&java=1&gears=0&ag=0&res=1920x1080&cookie=1"
  "The CloudFront line %s".format(line1) should {

    val event = SnowPlowEventDeserializer.deserializeLine(line1, true)

    // Check main type
    "deserialize as a SnowPlowEventStruct" in {
      event must beAnInstanceOf[SnowPlowEventStruct]
    }

    val eventStruct = event.asInstanceOf[SnowPlowEventStruct]

    // Check all of the field types
    "deserialize with a dt (Date) field which is a Hive STRING" in {
      eventStruct.dt must beAnInstanceOf[java.lang.String]
    }
    "deserialize with a tm (Time) field which is a Hive STRING" in {
      eventStruct.tm must beAnInstanceOf[java.lang.String]
    }
    "deserialize with a user_ipaddress (User IP Address) field which is a Hive STRING" in {
      eventStruct.user_ipaddress must beAnInstanceOf[java.lang.String]
    }
    "deserialize with a page_url (Page URL) field which is a Hive STRING" in {
      eventStruct.page_url must beAnInstanceOf[java.lang.String]
    }
    // TODO
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Value checks
  // -------------------------------------------------------------------------------------------------------------------

  // Now let's check the specific values for another couple of lines
  // TODO
}
