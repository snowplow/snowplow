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
package com.snowplowanalytics.snowplow.hadoop.hive

// Specs2
import org.specs2.mutable.Specification

// Hive
import org.apache.hadoop.hive.serde2.SerDeException;

class StructTypeTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  val types = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&uid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"

  "The CloudFront row \"%s\"".format(types) should {

    val event = SnowPlowEventDeserializer.deserializeLine(types, DEBUG)

    // Check main type
    "deserialize as a SnowPlowEventStruct" in {
      event must beAnInstanceOf[SnowPlowEventStruct]
    }

    val eventStruct = event.asInstanceOf[SnowPlowEventStruct]

    // Check all of the field types

    // Date/time
    "with a dt (Date) field which is a Hive STRING" in {
      eventStruct.dt must beAnInstanceOf[java.lang.String]
    }
    "with a tm (Time) field which is a Hive STRING" in {
      eventStruct.tm must beAnInstanceOf[java.lang.String]
    }

    // User and visit
    "with a user_ipaddress (User IP Address) field which is a Hive STRING" in {
      eventStruct.user_ipaddress must beAnInstanceOf[java.lang.String]
    }

    // Page
    "with a page_url (Page URL) field which is a Hive STRING" in {
      eventStruct.page_url must beAnInstanceOf[java.lang.String]
    }

    // Browser (from user-agent)
    "with a br_name (Browser Name) field which is a Hive STRING" in {
      eventStruct.br_name must beAnInstanceOf[java.lang.String]
    }
    "with a br_family (Browser Family) field which is a Hive STRING" in {
      eventStruct.br_family must beAnInstanceOf[java.lang.String]
    }
    "with a br_version (Browser Version) field which is a Hive STRING" in {
      eventStruct.br_version must beAnInstanceOf[java.lang.String]
    }
    "with a br_type (Browser Type) field which is a Hive STRING" in {
      eventStruct.br_type must beAnInstanceOf[java.lang.String]
    }
    "with a br_renderengine (Browser Rendering Engine) field which is a Hive STRING" in {
      eventStruct.br_renderengine must beAnInstanceOf[java.lang.String]
    }
    "with a os_name (OS Name) field which is a Hive STRING" in {
      eventStruct.os_name must beAnInstanceOf[java.lang.String]
    }
    "with a os_family (OS Family) field which is a Hive STRING" in {
      eventStruct.os_family must beAnInstanceOf[java.lang.String]
    }
    "with a os_manufacturer (OS Manufacturer) field which is a Hive STRING" in {
      eventStruct.os_manufacturer must beAnInstanceOf[java.lang.String]
    }
    "with a dvce_ismobile (Device Is Mobile?) field which is a Hive BOOLEAN" in {
      eventStruct.dvce_ismobile must beAnInstanceOf[java.lang.Boolean]
    }
    "with a dvce_type (Device Type) field which is a Hive STRING" in {
      eventStruct.dvce_type must beAnInstanceOf[java.lang.String]
    }
    // TODO
  }
}