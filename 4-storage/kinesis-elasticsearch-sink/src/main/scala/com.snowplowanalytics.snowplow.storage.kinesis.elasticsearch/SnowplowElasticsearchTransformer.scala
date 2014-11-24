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

// TODO use a package object
import SnowplowRecord._

/**
 * Class to convert successfully enriched events to EmitterInputs
 */
class SnowplowElasticsearchTransformer(documentIndex: String, documentType: String) extends ITransformer[ValidatedRecord, EmitterInput] {

  private val LatitudeIndex = 22
  private val LongitudeIndex = 23

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
  private val tstampFields = Set(
    "etl_tstamp",
    "collector_tstamp",
    "dvce_tstamp"
    )

  /**
   * Convert the value of a field to a JValue based on the name of the field
   *
   * @param kvPair Tuple2 of the name and value of the field
   * @return JObject representing a single field in the JSON
   */
  private def converter(kvPair: (String, String)): ValidationNel[String, JObject] = {
    val (key, value) = kvPair
    if (value.isEmpty) {
      JObject(key -> JNull).successNel
    } else {
      try {
        if (intFields.contains(key)) {
          JObject(key -> JInt(value.toInt)).successNel
        } else if (doubleFields.contains(key)) {
          JObject(key -> JDouble(value.toDouble)).successNel
        } else if (boolFields.contains(key)) {
          value match {
            case "1" => JObject(key -> JBool(true)).successNel
            case "0" => JObject(key -> JBool(false)).successNel
            case _   => "Value [%s] is not valid for field [%s]: expected 0 or 1".format(value, key).failNel
          }
        } else if (tstampFields.contains(key)) {
          JObject(key -> JString(reformatTstamp(value))).successNel
        } else if (key == "contexts") {
          Shredder.parseContexts(value)
        } else if (key == "unstruct_event") {
          Shredder.parseUnstruct(value)
        } else {
          JObject(key -> JString(value)).successNel
        }
      } catch {
        case e @ (_ : IllegalArgumentException | _: JsonParseException) =>
          "Value [%s] is not valid for field [%s]: %s".format(value, key, e.getMessage).failNel
      }
    }
  }

  /**
   * Converts a timestamp to ISO 8601 format
   *
   * @param tstamp Timestamp of the form YYYY-MM-DD hh:mm:ss
   * @return ISO 8601 timestamp
   */
  private def reformatTstamp (tstamp: String): String = tstamp.replaceAll(" ", "T") + "Z"

  /**
   * Converts an aray of field values to a JSON whose keys are the field names
   *
   * @param event Array of values for the event
   * @return ValidatedRecord containing JSON for the event and the event_id (if it exists)
   */
  def jsonifyGoodEvent(event: Array[String]): ValidationNel[String, JsonRecord] = {
    if (event.size == fields.size) {
      val geoLocation: JObject = {
        val latitude = event(LatitudeIndex)
        val longitude = event(LongitudeIndex)
        if (latitude.size > 0 && longitude.size > 0) {
          JObject("geo_location" -> JString(s"$longitude,$latitude"))
        } else {
          JObject()
        }
      }
      val validatedJObjects: Array[ValidationNel[String, JObject]] = fields.zip(event).map(converter)
      val switched: ValidationNel[String, List[JObject]] = validatedJObjects.toList.sequenceU
      switched.map( x => {
        val j = x.fold(geoLocation)(_ ~ _)
        JsonRecord(compact(render(j)), ScalazJson4sUtils.extract[String](j, "event_id").toOption)
      })
    } else {
      "Event does not have the correct number of fields: expected %s, found %s"
        .format(fields.size, event.size).failNel
    }
  }

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedRecord = {
    val recordString = new String(record.getData.array) // TODO: just use the original record

    // The -1 is necessary to prevent trailing empty strings from being discarded
    (recordString, jsonifyGoodEvent(recordString.split("\t", -1)).leftMap(_.toList))
  }

  /**
   * Convert a buffered event JSON to an EmitterInput
   *
   * @param record ValidatedRecord containing a good event JSON
   * @return An EmitterInput
   */
  override def fromClass(record: ValidatedRecord): EmitterInput =
    record.map(_.map(r => r.id match {
      case Some(id) => new ElasticsearchObject(documentIndex, documentType, id, r.json)
      case None => new ElasticsearchObject(documentIndex, documentType, r.json)
    }))

}
