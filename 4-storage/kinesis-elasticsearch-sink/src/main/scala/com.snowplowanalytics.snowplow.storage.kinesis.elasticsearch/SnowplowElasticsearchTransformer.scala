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
package com.snowplowanalytics.snowplow
package storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchObject,
  ElasticsearchTransformer
}
import com.amazonaws.services.kinesis.model.Record

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.matching.Regex

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

// Snowplow
import enrich.common.utils.ScalazJson4sUtils

/**
 * Class to convert successfully enriched events to ElasticsearchObjects
 */
class SnowplowElasticsearchTransformer(documentIndex: String, documentType: String) extends ElasticsearchTransformer[JsonRecord]
  with ITransformer[JsonRecord, ElasticsearchObject] {

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
    "ip_organization",
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
    "se_value",
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
    "doc_height"
    )

  private val intFields = Set(
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
  private val doubleFields = Set(
    "geo_latitude",
    "geo_longitude",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "ti_price"
    )
  private val boolFields = Set(
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
    "dvce_ismobile"
    )

  private def fixSchema(prefix: String, schema: String): String = {
    val schemaPatter = """.+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""".r
    schema match {
      case schemaPatter(organization, name, schemaVer) => {
        val snakeCaseOrganization = organization.replaceAll("""\.""", "_").toLowerCase
        val snakeCaseName = name.replaceAll("([^_])([A-Z])", "$1_$2").toLowerCase
        val model = schemaVer.split("-")(0)
        s"${prefix}_${snakeCaseOrganization}_${snakeCaseName}_${model}"
      }
      // TODO decide whether we want to fall back to the original schema string
      case _ => s"${prefix}.${schema}"
    }
  }

  private def parseContexts(contexts: String): JObject = {
    val json = parse(contexts)
    val data = json \ "data"
    data.children.map(context => (context \ "schema", context \ "data")).collect({
      case (JString(schema), innerData) if innerData != JNothing => (fixSchema("contexts", schema), innerData)
    }).groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2)))
  }

  private def parseUnstruct(unstruct: String): JObject = {
    val json = parse(unstruct)
    val data = json \ "data"
    val schema = data \ "schema"
    val innerData = data \ "data"
    val fixedSchema = schema match {
      case JString(s) => fixSchema("unstruct", s)
      case _ => throw new RuntimeException("TODO: badly formatted event")
    }
    (fixedSchema, innerData)
  }

  /**
   * Convert the value of a field to a JValue based on the name of the field
   *
   * @param kvPair Tuple2 of the name and value of the field
   * @return JObject representing a single field in the JSON
   */
  private def converter(kvPair: (String, String)): JObject = {
    val (key, value) = kvPair
    if (value.isEmpty) {
      (key, JNull)
    } else {
      try {
        if (intFields.contains(key)) {
          (key, JInt(value.toInt))
        } else if (doubleFields.contains(key)) {
          (key, JDouble(value.toDouble))
        } else if (boolFields.contains(key)) {
          (key, value match {
            case "1" => JBool(true)
            case "0" => JBool(false)
            case s   => JString(s)
          })
        } else if (key == "contexts") {
          parseContexts(value)
        } else if (key == "unstruct_event") {
          parseUnstruct(value)
        } else {
          (key, JString(value))
        }
      } catch {
        case iae: IllegalArgumentException => (key, JString(value)) // TODO: log the exception
        case jpe: JsonParseException => (key, JString(value)) // TODO: log the exception
      }
    }
  }

  /**
   * Converts an aray of field values to a JSON whose keys are the field names
   *
   * @param event Array of values for the event
   * @return JsonRecord containing JSON for the event and the event_id (if it exists)
   */
  def jsonifyGoodEvent(event: Array[String]): JsonRecord = {
    val jObjects: Array[JObject] = fields.zip(event).map(converter)
    val eventJson = jObjects.fold(JObject())(_ ~ _)
    JsonRecord(compact(render(eventJson)), ScalazJson4sUtils.extract[String](eventJson, "event_id").toOption)
  }

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return JsonRecord for the event
   */
  override def toClass(record: Record): JsonRecord =
    jsonifyGoodEvent(new String(record.getData.array).split("\t"))

  /**
   * Convert a buffered event JSON to an ElasticsearchObject
   *
   * @param record JsonRecord containing a good event JSON
   * @return An ElasticsearchObject
   */
  override def fromClass(record: JsonRecord): ElasticsearchObject =
    record.id match {
      case Some(id) => new ElasticsearchObject(documentIndex, documentType, id, record.json)
      case None => new ElasticsearchObject(documentIndex, documentType, record.json)
    }

}
