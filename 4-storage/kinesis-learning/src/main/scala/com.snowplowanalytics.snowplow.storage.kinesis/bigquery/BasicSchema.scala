 /*
 * Copyright (c) 2015 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

//TODO - Change "STRING", "INT", etc to algebraic data types

object SnowplowEnrichedEventSchema{
  val fields = Array(
    ("app_id", "STRING"),
    ("platform", "STRING"),
    ("etl_tstamp", "TIMESTAMP"),
    ("collector_tstamp", "TIMESTAMP"),
    ("dvce_tstamp", "TIMESTAMP"),
    ("event", "STRING"),
    ("event_id", "STRING"),
    ("txn_id", "INTEGER"),
    ("name_tracker", "STRING"),
    ("v_tracker", "STRING"),
    ("v_collector", "STRING"),
    ("v_etl", "STRING"),
    ("user_id", "STRING"),
    ("user_ipaddress", "STRING"),
    ("user_fingerprint", "STRING"),
    ("domain_userid", "STRING"),
    ("domain_sessionidx","INTEGER"),
    ("network_userid", "STRING"),
    ("geo_country", "STRING"),
    ("geo_region", "STRING"),
    ("geo_city", "STRING"),
    ("geo_zipcode", "STRING"),
    ("geo_latitude", "FLOAT"),
    ("geo_longitude", "FLOAT"),
    ("geo_region_name", "STRING"),
    ("ip_isp", "STRING"),
    ("ip_organization", "STRING"),
    ("ip_domain", "STRING"),
    ("ip_netspeed", "STRING"),
    ("page_url", "STRING"),
    ("page_title", "STRING"),
    ("page_referrer", "STRING"),
    ("page_urlscheme", "STRING"),
    ("page_urlhost", "STRING"),
    ("page_urlport","INTEGER"),
    ("page_urlpath", "STRING"),
    ("page_urlquery", "STRING"),
    ("page_urlfragment", "STRING"),
    ("refr_urlscheme", "STRING"),
    ("refr_urlhost", "STRING"),
    ("refr_urlport","INTEGER"),
    ("refr_urlpath", "STRING"),
    ("refr_urlquery", "STRING"),
    ("refr_urlfragment", "STRING"),
    ("refr_medium", "STRING"),
    ("refr_source", "STRING"),
    ("refr_term", "STRING"),
    ("mkt_medium", "STRING"),
    ("mkt_source", "STRING"),
    ("mkt_term", "STRING"),
    ("mkt_content", "STRING"),
    ("mkt_campaign", "STRING"),
    ("contexts", "STRING"),
    ("se_category", "STRING"),
    ("se_action", "STRING"),
    ("se_label", "STRING"),
    ("se_property", "STRING"),
    ("se_value", "FLOAT"),
    ("unstruct_event", "STRING"),
    ("tr_orderid", "STRING"),
    ("tr_affiliation", "STRING"),
    ("tr_total", "FLOAT"),
    ("tr_tax", "FLOAT"),

    ("tr_shipping", "FLOAT"),
    ("tr_city", "STRING"),
    ("tr_state", "STRING"),
    ("tr_country", "STRING"),
    ("ti_orderid", "STRING"),
    ("ti_sku", "STRING"),
    ("ti_name", "STRING"),
    ("ti_category", "STRING"),
    ("ti_price", "FLOAT"),
    ("ti_quantity", "INTEGER"),
    ("pp_xoffset_min", "INTEGER"),
    ("pp_xoffset_max", "INTEGER"),
    ("pp_yoffset_min", "INTEGER"),
    ("pp_yoffset_max", "INTEGER"),
    ("useragent", "STRING"),
    ("br_name", "STRING"),
    ("br_family", "STRING"),
    ("br_version", "STRING"),
    ("br_type", "STRING"),
    ("br_renderengine", "STRING"),
    ("br_lang", "STRING"),
    ("br_features_pdf", "BOOLEAN"),
    ("br_features_flash", "BOOLEAN"),
    ("br_features_java", "BOOLEAN"),
    ("br_features_director", "BOOLEAN"),
    ("br_features_quicktime", "BOOLEAN"),
    ("br_features_realplayer", "BOOLEAN"),
    ("br_features_windowsmedia", "BOOLEAN"),
    ("br_features_gears", "BOOLEAN"),
    ("br_features_silverlight", "BOOLEAN"),
    ("br_cookies", "BOOLEAN"),
    ("br_colordepth", "STRING"),
    ("br_viewwidth", "INTEGER"),
    ("br_viewheight", "INTEGER"),
    ("os_name", "STRING"),
    ("os_family", "STRING"),
    ("os_manufacturer", "STRING"),
    ("os_timezone", "STRING"),
    ("dvce_type", "STRING"),
    ("dvce_ismobile", "BOOLEAN"),
    ("dvce_screenwidth", "INTEGER"),
    ("dvce_screenheight", "INTEGER"),
    ("doc_charset", "STRING"),
    ("doc_width", "INTEGER"),
    ("doc_height", "INTEGER")
    )

    def names: Array[String] = {
      fields.map(_._1)
    }

    def types: Array[String] = {
      fields.map(_._2)
    }
}
