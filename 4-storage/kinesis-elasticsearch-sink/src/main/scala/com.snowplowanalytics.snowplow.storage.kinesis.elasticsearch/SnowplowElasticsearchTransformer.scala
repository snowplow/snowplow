 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchObject,
  ElasticsearchTransformer
}
import com.amazonaws.services.kinesis.model.Record

// Scala
import scala.util.parsing.json.JSONObject

class SnowplowElasticsearchTransformer extends ElasticsearchTransformer[String]
  with ITransformer[String, ElasticsearchObject] {

  private val fields = Array(
    "app_id",
    "platform",
    "etl_tstamp",
    "collector_tstamp",
    "dvce_tstamp",
    "event",
    "event_id",
    "txn_id",
    "name_tracker",
    "v_tracker",
    "v_collector",
    "v_etl",
    "user_id",
    "user_ipaddress",
    "user_fingerprint",
    "domain_userid",
    "domain_sessionidx",
    "network_userid",
    "geo_country",
    "geo_region",
    "geo_city",
    "geo_zipcode",
    "geo_latitude",
    "geo_longitude",
    "geo_region_name",
    "ip_isp",
    "ip_org",
    "ip_domain",
    "ip_netspeed",
    "page_url",
    "page_title",
    "page_referrer",
    "page_urlscheme",
    "page_urlhost",
    "page_urlport",
    "page_urlpath",
    "page_urlquery",
    "page_urlfragment",
    "refr_urlscheme",
    "refr_urlhost",
    "refr_urlport",
    "refr_urlpath",
    "refr_urlquery",
    "refr_urlfragment",
    "refr_medium",
    "refr_source",
    "refr_term",
    "mkt_medium",
    "mkt_source",
    "mkt_term",
    "mkt_content",
    "mkt_campaign",
    "contexts",
    "se_category",
    "se_action",
    "se_label",
    "se_property",
    "unstruct_event",
    "tr_orderid",
    "tr_affiliation",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "tr_city",
    "tr_state",
    "tr_country",
    "ti_orderid",
    "ti_sku",
    "ti_name",
    "ti_category",
    "ti_price",
    "ti_quantity",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "useragent",
    "br_name",
    "br_family",
    "br_version",
    "br_type",
    "br_renderengine",
    "br_lang",
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears",
    "br_features_silverlight",
    "br_cookies",
    "br_colordepth",
    "br_viewwidth",
    "br_viewheight",
    "os_name",
    "os_family",
    "os_manufacturer",
    "os_timezone",
    "dvce_type",
    "dvce_ismobile",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_charset",
    "doc_width",
    "doc_height")

  private val intFields = new Set(
    "txn_id",
    "domain_sessionidx",
    "page_urlport",
    "refr_urlport",
    "ti_quantity",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "br_viewwidth",
    "br_viewheight",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_width",
    "doc_height"
    )
  private val doubleFields = new Set(
    "geo_latitude",
    "geo_longitude",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "ti_price",
    )
  private val boolFields = new Set(
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears boolean",
    "br_features_silverlight",
    "br_cookies",
    "dvce_ismobile"
    )

  private val converter: (entry: (String, String) => (String, Any)) = e => {
    try {
      if (intFields.contains(e._1)) {
        (e._1, e._2.toInt)
      } else if (doubleFields.contains(e._1)) {
        (e._1, e._2.toDouble)
      } else if boolFields.contains(e._1) {
        (e._1, e._2.toBoolean)
      } else {
          e
      }
    catch {
      IllegalArgumentException(iae) => e // TODO: log the exception
    }
  }

  override def toClass(record: Record): String = {

    val fieldValues = new String(record.getData.array).split("\t")

    val fieldsMap = fields.zip(fieldValues).filter(! _._2.isEmpty).map(converter).toMap

    JSONObject(fieldsMap).toString()
  }

  override def fromClass(record: String): ElasticsearchObject  =  {

    val e = new ElasticsearchObject("events", "sometype", "event", record)
    e.setCreate(true)
    e
  }

}
