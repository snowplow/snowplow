/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package outputs

// Java
import java.lang.{Integer => JInteger}
import java.lang.{Float   => JFloat}
import java.lang.{Byte    => JByte}

// Scala
import scala.beans.BeanProperty

/**
 * The canonical output format for enriched events.
 *
 * For simplicity, we are using our Redshift format
 * as the canonical format, i.e. the below is
 * equivalent to the redshift-etl.q HiveQL script
 * used by the Hive ETL.
 *
 * When we move to Avro, we will
 * probably review some of these
 * types (e.g. move back to
 * Array for browser features, and
 * switch remaining Bytes to Booleans).
 */
// TODO: make the EnrichedEvent Avro-format, not Redshift-specific
class EnrichedEvent extends Serializable {

  // The application (site, game, app etc) this event belongs to, and the tracker platform
  @BeanProperty var app_id: String   = _
  @BeanProperty var platform: String = _

  // Date/time
  @BeanProperty var etl_tstamp: String          = _
  @BeanProperty var collector_tstamp: String    = _
  @BeanProperty var dvce_created_tstamp: String = _

  // Transaction (i.e. this logging event)
  @BeanProperty var event: String    = _
  @BeanProperty var event_id: String = _
  @BeanProperty var txn_id: String   = _

  // Versioning
  @BeanProperty var name_tracker: String = _
  @BeanProperty var v_tracker: String    = _
  @BeanProperty var v_collector: String  = _
  @BeanProperty var v_etl: String        = _

  // User and visit
  @BeanProperty var user_id: String             = _
  @BeanProperty var user_ipaddress: String      = _
  @BeanProperty var user_fingerprint: String    = _
  @BeanProperty var domain_userid: String       = _
  @BeanProperty var domain_sessionidx: JInteger = _
  @BeanProperty var network_userid: String      = _

  // Location
  @BeanProperty var geo_country: String     = _
  @BeanProperty var geo_region: String      = _
  @BeanProperty var geo_city: String        = _
  @BeanProperty var geo_zipcode: String     = _
  @BeanProperty var geo_latitude: JFloat    = _
  @BeanProperty var geo_longitude: JFloat   = _
  @BeanProperty var geo_region_name: String = _

  // Other IP lookups
  @BeanProperty var ip_isp: String          = _
  @BeanProperty var ip_organization: String = _
  @BeanProperty var ip_domain: String       = _
  @BeanProperty var ip_netspeed: String     = _

  // Page
  @BeanProperty var page_url: String      = _
  @BeanProperty var page_title: String    = _
  @BeanProperty var page_referrer: String = _

  // Page URL components
  @BeanProperty var page_urlscheme: String   = _
  @BeanProperty var page_urlhost: String     = _
  @BeanProperty var page_urlport: JInteger   = _
  @BeanProperty var page_urlpath: String     = _
  @BeanProperty var page_urlquery: String    = _
  @BeanProperty var page_urlfragment: String = _

  // Referrer URL components
  @BeanProperty var refr_urlscheme: String   = _
  @BeanProperty var refr_urlhost: String     = _
  @BeanProperty var refr_urlport: JInteger   = _
  @BeanProperty var refr_urlpath: String     = _
  @BeanProperty var refr_urlquery: String    = _
  @BeanProperty var refr_urlfragment: String = _

  // Referrer details
  @BeanProperty var refr_medium: String = _
  @BeanProperty var refr_source: String = _
  @BeanProperty var refr_term: String   = _

  // Marketing
  @BeanProperty var mkt_medium: String   = _
  @BeanProperty var mkt_source: String   = _
  @BeanProperty var mkt_term: String     = _
  @BeanProperty var mkt_content: String  = _
  @BeanProperty var mkt_campaign: String = _

  // Custom Contexts
  @BeanProperty var contexts: String = _

  // Structured Event
  @BeanProperty var se_category: String = _
  @BeanProperty var se_action: String   = _
  @BeanProperty var se_label: String    = _
  @BeanProperty var se_property: String = _
  @BeanProperty var se_value
    : String = _ // Technically should be a Double but may be rendered incorrectly by Cascading with scientific notification (which Redshift can't process)

  // Unstructured Event
  @BeanProperty var unstruct_event: String = _

  // Ecommerce transaction (from querystring)
  @BeanProperty var tr_orderid: String     = _
  @BeanProperty var tr_affiliation: String = _
  @BeanProperty var tr_total: String       = _
  @BeanProperty var tr_tax: String         = _
  @BeanProperty var tr_shipping: String    = _
  @BeanProperty var tr_city: String        = _
  @BeanProperty var tr_state: String       = _
  @BeanProperty var tr_country: String     = _

  // Ecommerce transaction item (from querystring)
  @BeanProperty var ti_orderid: String    = _
  @BeanProperty var ti_sku: String        = _
  @BeanProperty var ti_name: String       = _
  @BeanProperty var ti_category: String   = _
  @BeanProperty var ti_price: String      = _
  @BeanProperty var ti_quantity: JInteger = _

  // Page Pings
  @BeanProperty var pp_xoffset_min: JInteger = _
  @BeanProperty var pp_xoffset_max: JInteger = _
  @BeanProperty var pp_yoffset_min: JInteger = _
  @BeanProperty var pp_yoffset_max: JInteger = _

  // User Agent
  @BeanProperty var useragent: String = _

  // Browser (from user-agent)
  @BeanProperty var br_name: String         = _
  @BeanProperty var br_family: String       = _
  @BeanProperty var br_version: String      = _
  @BeanProperty var br_type: String         = _
  @BeanProperty var br_renderengine: String = _

  // Browser (from querystring)
  @BeanProperty var br_lang: String = _
  // Individual feature fields for non-Hive targets (e.g. Infobright)
  @BeanProperty var br_features_pdf: JByte          = _
  @BeanProperty var br_features_flash: JByte        = _
  @BeanProperty var br_features_java: JByte         = _
  @BeanProperty var br_features_director: JByte     = _
  @BeanProperty var br_features_quicktime: JByte    = _
  @BeanProperty var br_features_realplayer: JByte   = _
  @BeanProperty var br_features_windowsmedia: JByte = _
  @BeanProperty var br_features_gears: JByte        = _
  @BeanProperty var br_features_silverlight: JByte  = _
  @BeanProperty var br_cookies: JByte               = _
  @BeanProperty var br_colordepth: String           = _
  @BeanProperty var br_viewwidth: JInteger          = _
  @BeanProperty var br_viewheight: JInteger         = _

  // OS (from user-agent)
  @BeanProperty var os_name: String         = _
  @BeanProperty var os_family: String       = _
  @BeanProperty var os_manufacturer: String = _
  @BeanProperty var os_timezone: String     = _

  // Device/Hardware (from user-agent)
  @BeanProperty var dvce_type: String    = _
  @BeanProperty var dvce_ismobile: JByte = _

  // Device (from querystring)
  @BeanProperty var dvce_screenwidth: JInteger  = _
  @BeanProperty var dvce_screenheight: JInteger = _

  // Document
  @BeanProperty var doc_charset: String  = _
  @BeanProperty var doc_width: JInteger  = _
  @BeanProperty var doc_height: JInteger = _

  // Currency
  @BeanProperty var tr_currency: String      = _
  @BeanProperty var tr_total_base: String    = _
  @BeanProperty var tr_tax_base: String      = _
  @BeanProperty var tr_shipping_base: String = _
  @BeanProperty var ti_currency: String      = _
  @BeanProperty var ti_price_base: String    = _
  @BeanProperty var base_currency: String    = _

  // Geolocation
  @BeanProperty var geo_timezone: String = _

  // Click ID
  @BeanProperty var mkt_clickid: String = _
  @BeanProperty var mkt_network: String = _

  // ETL tags
  @BeanProperty var etl_tags: String = _

  // Time event was sent
  @BeanProperty var dvce_sent_tstamp: String = _

  // Referer
  @BeanProperty var refr_domain_userid: String = _
  @BeanProperty var refr_dvce_tstamp: String   = _

  // Derived contexts
  @BeanProperty var derived_contexts: String = _

  // Session ID
  @BeanProperty var domain_sessionid: String = _

  // Derived timestamp
  @BeanProperty var derived_tstamp: String = _

  // Derived event vendor/name/format/version
  @BeanProperty var event_vendor: String  = _
  @BeanProperty var event_name: String    = _
  @BeanProperty var event_format: String  = _
  @BeanProperty var event_version: String = _

  // Event fingerprint
  @BeanProperty var event_fingerprint: String = _

  // True timestamp
  @BeanProperty var true_tstamp: String = _

  // Fields modified in PII enrichemnt (JSON String)
  @BeanProperty var pii: String = _
}
