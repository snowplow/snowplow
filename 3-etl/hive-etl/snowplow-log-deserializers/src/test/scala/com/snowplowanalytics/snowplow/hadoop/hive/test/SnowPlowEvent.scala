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

/**
 * Scala-friendly equivalent of SnowPlowEventStruct.
 * Can't use a case class because we have too many
 * fields (also some tests just need a few fields
 * set).
 */
class SnowPlowEvent {

  // Date/time
  var dt: String = _
  var tm: String = _

  // Transaction (i.e. this logging event)
  var txn_id: String = _

  // The application (site, game, app etc) this event belongs to
  var app_id: String = _

  // User and visit
  var user_id: String = _
  var user_ipaddress: String = _
  var visit_id: Int = _

  // Page
  var page_url: String = _
  var page_title: String = _
  var page_referrer: String = _

  // Marketing
  var mkt_medium: String = _
  var mkt_source: String = _
  var mkt_term: String = _
  var mkt_content: String = _
  var mkt_campaign: String = _

  // Event
  var ev_category: String = _
  var ev_action: String = _
  var ev_label: String = _
  var ev_property: String = _
  var ev_value: String = _

  // Browser (from user-agent)
  var br_name: String = _
  var br_family: String = _
  var br_version: String = _
  var br_type: String = _
  var br_renderengine: String = _

  // Browser (from querystring)
  var br_lang: String = _
  var br_features: List[String] = List[String]()
  // Individual feature fields for non-Hive targets (e.g. Infobright)
  var br_features_pdf: Boolean = _
  var br_features_flash: Boolean = _
  var br_features_java: Boolean = _
  var br_features_director: Boolean = _
  var br_features_quicktime: Boolean = _
  var br_features_realplayer: Boolean = _
  var br_features_windowsmedia: Boolean = _
  var br_features_gears: Boolean = _
  var br_features_silverlight: Boolean = _
  var br_cookies: Boolean = _

  // OS (from user-agent)
  var os_name: String = _
  var os_family: String = _
  var os_manufacturer: String = _

  // Device/Hardware (from user-agent)
  var dvce_type: String = _
  var dvce_ismobile: Boolean = _

  // Device (from querystring)
  var dvce_screenwidth: Int = _
  var dvce_screenheight: Int = _

  // Ecommerce transaction (from querystring)
  var tr_orderid: String = _
  var tr_affiliation: String = _
  var tr_total: String = _
  var tr_tax: String = _
  var tr_shipping: String = _
  var tr_city: String = _
  var tr_state: String = _
  var tr_country: String = _

  // Ecommerce transaction item (from querystring)
  var ti_orderid: String = _
  var ti_sku: String = _
  var ti_name: String = _
  var ti_category: String = _
  var ti_price: String = _
  var ti_quantity: String = _
}