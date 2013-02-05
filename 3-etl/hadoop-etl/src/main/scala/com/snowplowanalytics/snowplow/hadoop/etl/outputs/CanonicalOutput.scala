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
package com.snowplowanalytics.snowplow.hadoop.etl
package outputs

// Scala
import scala.reflect.BeanProperty

/**
 * The canonical output format.
 *
 * For simplicity, we are using our
 * "non-Hive" format as the canonical
 * format. I.e. the below is
 * equivalent to the
 * non-hive-rolling-etl.q HiveQL
 * script used by the Hive ETL.
 *
 * When we move to Avro, we will
 * probably review some of these
 * types (e.g. move back to
 * Array for browser features, and
 * switch remaining Bytes to Booleans.
 */
class CanonicalOutput {

  // The application (site, game, app etc) this event belongs to, and the tracker platform
  var app_id: String = _
  var platform: String = _

  // Date/time
  var dt: String = _
  var tm: String = _

  // Transaction (i.e. this logging event)
  var event: String = _
  var event_id: String = _
  var txn_id: String = _

  // Versioning
  var v_tracker: String = _
  var v_collector: String = _
  var v_etl: String = _

  // User and visit
  var user_id: String = _
  var user_ipaddress: String = _
  var user_fingerprint: String = _
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
  var br_cookies: Byte = _
  var br_colordepth: String = _

  // OS (from user-agent)
  var os_name: String = _
  var os_family: String = _
  var os_manufacturer: String = _
  var os_timezone: String = _

  // Device/Hardware (from user-agent)
  var dvce_type: String = _
  var dvce_ismobile: Byte = _

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