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

/**
 * The Hive-friendly format generated
 * by the ETL process. Equivalent to
 * the hive-rolling-etl.q HiveQL
 * script used by the Hive ETL. 
 */
class HiveOutput {

  // Columns are ordered as added.
  // This is so that HiveQL queries
  // can work across file versions.
  var tm: String = _
  var txn_id: String = _
  var user_id: String = _
  var user_ipaddress: String = _
  var visit_id: Int = _
  var page_url: String = _
  var page_title: String = _
  var page_referrer: String = _
  var mkt_source: String = _
  var mkt_medium: String = _
  var mkt_term: String = _
  var mkt_content: String = _
  var mkt_campaign: String = _
  var ev_category: String = _
  var ev_action: String = _
  var ev_label: String = _
  var ev_property: String = _
  var ev_value: String = _
  var tr_orderid: String = _
  var tr_affiliation: String = _
  var tr_total: String = _
  var tr_tax: String = _
  var tr_shipping: String = _
  var tr_city: String = _
  var tr_state: String = _
  var tr_country: String = _
  var ti_orderid: String = _
  var ti_sku: String = _
  var ti_name: String = _
  var ti_category: String = _
  var ti_price: String = _
  var ti_quantity: String = _
  var br_name: String = _
  var br_family: String = _
  var br_version: String = _
  var br_type: String = _
  var br_renderengine: String = _
  var br_lang: String = _
  var br_features: List[String] = List[String]()
  var br_cookies: Boolean = _
  var os_name: String = _
  var os_family: String = _
  var os_manufacturer: String = _
  var dvce_type: String = _
  var dvce_ismobile: Boolean = _
  var dvce_screenwidth: Int = _
  var dvce_screenheight: Int = _
  var app_id: String = _
  var platform: String = _
  var event: String = _
  var v_tracker: String = _
  var v_collector: String = _
  var v_etl: String = _
  var event_id: String = _
  var user_fingerprint: String = _
  var useragent: String = _
  var br_colordepth: String = _
  var os_timezone: String = _
}