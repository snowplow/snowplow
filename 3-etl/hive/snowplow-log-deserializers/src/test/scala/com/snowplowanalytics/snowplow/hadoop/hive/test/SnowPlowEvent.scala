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
package test

// Scala
import scala.reflect.BeanProperty

/**
 * Scala-friendly equivalent of SnowPlowEventStruct.
 * Can't use a case class because we have too many
 * fields (also some tests just need a few fields
 * set).
 */
class SnowPlowEvent {

  // Date/time
  @BeanProperty
  var dt: String = _
  @BeanProperty
  var tm: String = _

  // Transaction (i.e. this logging event)
  @BeanProperty
  var txn_id: String = _

  // The application (site, game, app etc) this event belongs to
  var app_id: String = _

  // User and visit
  @BeanProperty
  var user_id: String = _
  @BeanProperty
  var user_ipaddress: String = _
  @BeanProperty
  var visit_id: Int = _

  // Page
  @BeanProperty
  var page_url: String = _
  @BeanProperty
  var page_title: String = _
  @BeanProperty
  var page_referrer: String = _

  // Marketing
  @BeanProperty
  var mkt_medium: String = _
  @BeanProperty
  var mkt_source: String = _
  @BeanProperty
  var mkt_term: String = _
  @BeanProperty
  var mkt_content: String = _
  @BeanProperty
  var mkt_campaign: String = _

  // Event
  @BeanProperty
  var ev_category: String = _
  @BeanProperty
  var ev_action: String = _
  @BeanProperty
  var ev_label: String = _
  @BeanProperty
  var ev_property: String = _
  @BeanProperty
  var ev_value: String = _

  // Browser (from user-agent)
  @BeanProperty
  var br_name: String = _
  @BeanProperty
  var br_family: String = _
  @BeanProperty
  var br_version: String = _
  @BeanProperty
  var br_type: String = _
  @BeanProperty
  var br_renderengine: String = _

  // Browser (from querystring)
  @BeanProperty
  var br_lang: String = _
  @BeanProperty
  var br_features: List[String] = List[String]()
  @BeanProperty
  var br_cookies: Boolean = _

  // OS (from user-agent)
  @BeanProperty
  var os_name: String = _
  @BeanProperty
  var os_family: String = _
  @BeanProperty
  var os_manufacturer: String = _

  // Device/Hardware (from user-agent)
  @BeanProperty
  var dvce_type: String = _
  @BeanProperty
  var dvce_ismobile: Boolean = _

  // Device (from querystring)
  @BeanProperty
  var dvce_screenwidth: Int = _
  @BeanProperty
  var dvce_screenheight: Int = _

  // Ecommerce transaction (from querystring)
  @BeanProperty
  var tr_orderid: String = _
  @BeanProperty
  var tr_affiliation: String = _
  @BeanProperty
  var tr_total: String = _
  @BeanProperty
  var tr_tax: String = _
  @BeanProperty
  var tr_shipping: String = _
  @BeanProperty
  var tr_city: String = _
  @BeanProperty
  var tr_state: String = _
  @BeanProperty
  var tr_country: String = _

  // Ecommerce transaction item (from querystring)
  @BeanProperty
  var ti_orderid: String = _
  @BeanProperty
  var ti_sku: String = _
  @BeanProperty
  var ti_name: String = _
  @BeanProperty
  var ti_category: String = _
  @BeanProperty
  var ti_price: String = _
  @BeanProperty
  var ti_quantity: String = _
}