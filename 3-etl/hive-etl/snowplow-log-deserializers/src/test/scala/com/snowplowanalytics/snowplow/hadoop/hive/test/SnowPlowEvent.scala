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
package test

/**
 * Scala-friendly equivalent of SnowPlowEventStruct.
 * Can't use a case class because we have too many
 * fields (also some tests just need a few fields
 * set).
 */
class SnowPlowEvent {

  // The application (site, game, app etc) this event belongs to, and the tracker platform
  var app_id: String = _
  var platform: String = _

  // Date/time
  var dt: String = _
  var collector_dt: String = _
  var collector_tm: String = _
  var dvce_dt: String = _
  var dvce_tm: String = _
  var dvce_epoch: Long = _

  // Transaction (i.e. this logging event)
  var event: String = _
  var event_vendor: String = _
  var event_id: String = _
  var txn_id: String = _

  // User and visit
  var user_id: String = _
  var user_ipaddress: String = _
  var user_fingerprint: String = _
  var domain_userid: String = _
  var domain_sessionidx: Int = _
  var network_userid: String = _

  // Page
  var page_url: String = _
  var page_title: String = _
  var page_referrer: String = _

  // Page URL
  var page_urlscheme: String = _
  var page_urlhost: String = _
  var page_urlport: Int = _
  var page_urlpath: String = _
  var page_urlquery: String = _
  var page_urlfragment: String = _

  // Marketing
  var mkt_medium: String = _
  var mkt_source: String = _
  var mkt_term: String = _
  var mkt_content: String = _
  var mkt_campaign: String = _

  // Structured event
  var ev_category: String = _
  var ev_action: String = _
  var ev_label: String = _
  var ev_property: String = _
  var ev_value: String = _

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

  // Page ping
  var pp_xoffset_min: Int = _
  var pp_xoffset_max: Int = _
  var pp_yoffset_min: Int = _
  var pp_yoffset_max: Int = _

  // User Agent
  var useragent: String = _

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
  var br_features_pdf: Byte = _
  var br_features_flash: Byte = _
  var br_features_java: Byte = _
  var br_features_director: Byte = _
  var br_features_quicktime: Byte = _
  var br_features_realplayer: Byte = _
  var br_features_windowsmedia: Byte = _
  var br_features_gears: Byte = _
  var br_features_silverlight: Byte = _
  var br_cookies: Boolean = _
  var br_cookies_bt: Byte = _
  var br_colordepth: String = _
  var br_viewwidth: Int = _
  var br_viewheight: Int = _

  // OS (from user-agent)
  var os_name: String = _
  var os_family: String = _
  var os_manufacturer: String = _
  var os_timezone: String = _

  // Device/Hardware (from user-agent)
  var dvce_type: String = _
  var dvce_ismobile: Boolean = _
  var dvce_ismobile_bt: Byte = _

  // Device (from querystring)
  var dvce_screenwidth: Int = _
  var dvce_screenheight: Int = _

  // Document fields
  var doc_charset: String = _
  var doc_width: Int = _
  var doc_height: Int = _
}