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

// Java
import java.util.{ArrayList => JArrayList}
import java.lang.{Integer => JInteger}
import java.lang.{Boolean => JBoolean}

// Specs2
import org.specs2.mutable.Specification

// Deserializer
import test.SnowPlowDeserializer

class StructTypingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row = "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net\t/ice.png\t200\thttps://test.psybazaar.com/shop/checkout/?utm_source=a&utm_medium=cpc&utm_campaign=uk-oracle-decks--angel-cards-text&utm_term=buy%2520angel%2520cards&utm_content=b\tMozilla/5.0%20(X11;%20Ubuntu;%20Linux%20x86_64;%20rv:11.0)%20Gecko/20100101%20Firefox/11.0\t&aid=3&page=Test&ev_ca=ecomm&ev_ac=checkout&ev_la=id_email&ev_va=Empty&ev_pr=ERROR&tid=236095&refr=http%253A%252F%252Ftest.psybazaar.com%252F&duid=135f6b7536aff045&lang=en-US&vid=5&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1&tr_id=a&tr_af=b&tr_tt=c&tr_tx=d&tr_sh=e&tr_ci=f&tr_st=g&tr_co=h&ti_id=i&ti_sk=j&ti_na=k&ti_ca=l&ti_pr=m&ti_qu=n"

  "The hypothetical (because it includes every possible field) CloudFront row \"%s\"".format(row) should {

    val event = SnowPlowDeserializer.deserializeUntyped(row)

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
      eventStruct.collector_tm must beAnInstanceOf[String]
    }

    // Transaction
    "with a txn_id (Transaction ID) field which is a Hive STRING" in {
      eventStruct.txn_id must beAnInstanceOf[String]
    }

    // Application
    "with an app_id (Application ID) field which is a Hive STRING" in {
      eventStruct.app_id must beAnInstanceOf[String]
    }

    // User and visit
    "with a domain_userid (Domain User ID) field which is a Hive STRING" in {
      eventStruct.domain_userid must beAnInstanceOf[String]
    }
    "with a user_ipaddress (User IP Address) field which is a Hive STRING" in {
      eventStruct.user_ipaddress must beAnInstanceOf[String]
    }
    "with a visit_id (User IP Address) field which is a Hive STRING" in {
      eventStruct.domain_sessionidx must beAnInstanceOf[JInteger]
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
    "with a mkt_medium (Marketing Medium) field which is a Hive STRING" in {
      eventStruct.mkt_medium must beAnInstanceOf[String]
    }
    "with a mkt_campaign (Marketing Campaign) field which is a Hive STRING" in {
      eventStruct.mkt_campaign must beAnInstanceOf[String]
    }
    "with a mkt_source (Marketing Source) field which is a Hive STRING" in {
      eventStruct.mkt_source must beAnInstanceOf[String]
    }
    "with a mkt_term (Marketing Term) field which is a Hive STRING" in {
      eventStruct.mkt_term must beAnInstanceOf[String]
    }
    "with a mkt_content (Marketing Content) field which is a Hive STRING" in {
      eventStruct.mkt_content must beAnInstanceOf[String]
    }

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

    // Ecommerce transaction
    "with a tr_orderid (Transaction Orderid) field which is a Hive STRING" in {
      eventStruct.tr_orderid must beAnInstanceOf[String]
    }
    "with a tr_affiliation (Transaction Affiliation) field which is a Hive STRING" in {
      eventStruct.tr_affiliation must beAnInstanceOf[String]
    }
    "with a tr_total (Transaction Total) field which is a Hive STRING" in {
      eventStruct.tr_total must beAnInstanceOf[String]
    }
    "with a tr_tax (Transaction Tax) field which is a Hive STRING" in {
      eventStruct.tr_tax must beAnInstanceOf[String]
    }
    "with a tr_shipping (Transaction Shipping) field which is a Hive STRING" in {
      eventStruct.tr_shipping must beAnInstanceOf[String]
    }
    "with a tr_city (Transaction City) field which is a Hive STRING" in {
      eventStruct.tr_city must beAnInstanceOf[String]
    }
    "with a tr_state (Transaction State) field which is a Hive STRING" in {
      eventStruct.tr_state must beAnInstanceOf[String]
    }
    "with a tr_country (Transaction Country) field which is a Hive STRING" in {
      eventStruct.tr_country must beAnInstanceOf[String]
    }

    // Ecommerce transaction item
    // In reality, no SnowPlow row would have an ecommerce transaction item set as well as an ecommerce transaction itself, but this test is to test types only
    "with a ti_orderid (Transaction Item Orderid) field which is a Hive STRING" in {
      eventStruct.ti_orderid must beAnInstanceOf[String]
    }
    "with a ti_sku (Transaction Item Sku) field which is a Hive STRING" in {
      eventStruct.ti_sku must beAnInstanceOf[String]
    }
    "with a ti_name (Transaction Item Name) field which is a Hive STRING" in {
      eventStruct.ti_name must beAnInstanceOf[String]
    }
    "with a ti_category (Transaction Item Category) field which is a Hive STRING" in {
      eventStruct.ti_category must beAnInstanceOf[String]
    }
    "with a ti_price (Transaction Item Price) field which is a Hive STRING" in {
      eventStruct.ti_price must beAnInstanceOf[String]
    }
    "with a ti_quantity (Transaction Item Quantity) field which is a Hive STRING" in {
      eventStruct.ti_quantity must beAnInstanceOf[String]
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
