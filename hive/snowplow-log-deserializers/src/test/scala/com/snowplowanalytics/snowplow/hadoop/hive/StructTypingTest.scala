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

// Java
import java.util.{ArrayList => JArrayList}
import java.lang.{Integer => JInteger}
import java.lang.{Boolean => JBoolean}

// Specs2
import org.specs2.mutable.Specification

class StructTypingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  val types = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&page=Test&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_va=Empty&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&uid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1"

  "The hypothetical (because it includes every possible field) CloudFront row \"%s\"".format(types) should {

    val event = SnowPlowEventDeserializer.deserializeLine(types, DEBUG)

    // Check main type
    "deserialize as a SnowPlowEventStruct" in {
      event must beAnInstanceOf[SnowPlowEventStruct]
    }

    val eventStruct = event.asInstanceOf[SnowPlowEventStruct]

    // Check all of the field types

    // Date/time
    "with a dt (Date) field which is a Hive STRING" in {
      eventStruct.dt must beAnInstanceOf[String]
    }
    "with a tm (Time) field which is a Hive STRING" in {
      eventStruct.tm must beAnInstanceOf[String]
    }

    // Transaction
    "with a txn_id (Transaction ID) field which is a Hive STRING" in {
      eventStruct.txn_id must beAnInstanceOf[String]      
    }

    // User and visit
    "with a user_id (User ID) field which is a Hive STRING" in {
      eventStruct.user_id must beAnInstanceOf[String]
    }
    "with a user_ipaddress (User IP Address) field which is a Hive STRING" in {
      eventStruct.user_ipaddress must beAnInstanceOf[String]
    }
    "with a visit_id (User IP Address) field which is a Hive STRING" in {
      eventStruct.visit_id must beAnInstanceOf[JInteger]
    }

    // Page
    "with a page_url (Page URL) field which is a Hive STRING" in {
      eventStruct.page_url must beAnInstanceOf[String]
    }
    // In reality, no SnowPlow row would have page_title set as well as ev_ fields, but this test is to test types only
    "with a page_title (Page Title) field which is a Hive STRING" in {
      eventStruct.page_title must beAnInstanceOf[String]
    }
    "with a page_referrer (Page Referrer) field which is a Hive STRING" in {
      eventStruct.page_referrer must beAnInstanceOf[String]
    }

    // Marketing
    // TODO

    // Event
    "with a ev_category (Event Category) field which is a Hive STRING" in {
      eventStruct.ev_category must beAnInstanceOf[String]
    }
    "with a ev_action (Event Action) field which is a Hive STRING" in {
      eventStruct.ev_action must beAnInstanceOf[String]
    }
    "with a ev_label (Event Label) field which is a Hive STRING" in {
      eventStruct.ev_label must beAnInstanceOf[String]
    }
    "with a ev_property (Event Property) field which is a Hive STRING" in {
      eventStruct.ev_property must beAnInstanceOf[String]
    }
    "with a ev_value (Event Value) field which is a Hive STRING" in {
      eventStruct.ev_value must beAnInstanceOf[String]
    }    

    // Browser (from user-agent)
    "with a br_name (Browser Name) field which is a Hive STRING" in {
      eventStruct.br_name must beAnInstanceOf[String]
    }
    "with a br_family (Browser Family) field which is a Hive STRING" in {
      eventStruct.br_family must beAnInstanceOf[String]
    }
    "with a br_version (Browser Version) field which is a Hive STRING" in {
      eventStruct.br_version must beAnInstanceOf[String]
    }
    "with a br_type (Browser Type) field which is a Hive STRING" in {
      eventStruct.br_type must beAnInstanceOf[String]
    }
    "with a br_renderengine (Browser Rendering Engine) field which is a Hive STRING" in {
      eventStruct.br_renderengine must beAnInstanceOf[String]
    }

    // Browser (from querystring)
    "with a br_lang (Browser Lang) field which is a Hive STRING" in {
      eventStruct.br_lang must beAnInstanceOf[String]
    }
    "with a br_cookies (Browser Cookies Enabled?) field which is a Hive BOOLEAN" in {
      eventStruct.br_cookies must beAnInstanceOf[JBoolean]
    }
    "with a br_features (Browser Features) field which is a Hive ARRAY<STRING>" in {
      eventStruct.br_features must beAnInstanceOf[JArrayList[String]]
    }

    // OS (from user-agent)    
    "with a os_name (OS Name) field which is a Hive STRING" in {
      eventStruct.os_name must beAnInstanceOf[String]
    }
    "with a os_family (OS Family) field which is a Hive STRING" in {
      eventStruct.os_family must beAnInstanceOf[String]
    }
    "with a os_manufacturer (OS Manufacturer) field which is a Hive STRING" in {
      eventStruct.os_manufacturer must beAnInstanceOf[String]
    }
    
    // Device/Hardware (from user-agent) 
    "with a dvce_type (Device Type) field which is a Hive STRING" in {
      eventStruct.dvce_type must beAnInstanceOf[String]
    }
    "with a dvce_ismobile (Device Is Mobile?) field which is a Hive BOOLEAN" in {
      eventStruct.dvce_ismobile must beAnInstanceOf[JBoolean]
    }

    // Device (from querystring)
    "with a dvce_screenwidth (Device Screen Width) field which is a Hive INT" in {
      eventStruct.dvce_screenwidth must beAnInstanceOf[JInteger]
    }
    "with a dvce_screenheight (Device Screen Height) field which is a Hive INT" in {
      eventStruct.dvce_screenheight must beAnInstanceOf[JInteger]
    }
  }
}