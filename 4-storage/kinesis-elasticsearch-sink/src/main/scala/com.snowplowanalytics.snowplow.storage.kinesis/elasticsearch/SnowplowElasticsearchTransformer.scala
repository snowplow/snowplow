 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
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

// Java
import java.nio.charset.StandardCharsets.UTF_8

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

// Logging
import org.slf4j.LoggerFactory

object SnowplowElasticsearchTransformer {

  private val StringField: TsvToJsonConverter = (key, value) => JObject(key -> JString(value)).successNel
  private val IntField: TsvToJsonConverter = (key, value) => JObject(key -> JInt(value.toInt)).successNel
  private val BoolField: TsvToJsonConverter = handleBooleanField
  private val DoubleField: TsvToJsonConverter = (key, value) => JObject(key -> JDouble(value.toDouble)).successNel
  private val TstampField: TsvToJsonConverter = (key, value) => JObject(key -> JString(reformatTstamp(value))).successNel
  private val ContextsField: TsvToJsonConverter = (key, value) => Shredder.parseContexts(value)
  private val UnstructField: TsvToJsonConverter = (key, value) => Shredder.parseUnstruct(value)

  private val fields = List(
    "app_id" -> StringField,
    "platform" -> StringField,
    "etl_tstamp" -> TstampField,
    "collector_tstamp" -> TstampField,
    "dvce_tstamp" -> TstampField,
    "event" -> StringField,
    "event_id" -> StringField,
    "txn_id" -> IntField,
    "name_tracker" -> StringField,
    "v_tracker" -> StringField,
    "v_collector" -> StringField,
    "v_etl" -> StringField,
    "user_id" -> StringField,
    "user_ipaddress" -> StringField,
    "user_fingerprint" -> StringField,
    "domain_userid" -> StringField,
    "domain_sessionidx" -> IntField,
    "network_userid" -> StringField,
    "geo_country" -> StringField,
    "geo_region" -> StringField,
    "geo_city" -> StringField,
    "geo_zipcode" -> StringField,
    "geo_latitude" -> DoubleField,
    "geo_longitude" -> DoubleField,
    "geo_region_name" -> StringField,
    "ip_isp" -> StringField,
    "ip_organization" -> StringField,
    "ip_domain" -> StringField,
    "ip_netspeed" -> StringField,
    "page_url" -> StringField,
    "page_title" -> StringField,
    "page_referrer" -> StringField,
    "page_urlscheme" -> StringField,
    "page_urlhost" -> StringField,
    "page_urlport" -> IntField,
    "page_urlpath" -> StringField,
    "page_urlquery" -> StringField,
    "page_urlfragment" -> StringField,
    "refr_urlscheme" -> StringField,
    "refr_urlhost" -> StringField,
    "refr_urlport" -> IntField,
    "refr_urlpath" -> StringField,
    "refr_urlquery" -> StringField,
    "refr_urlfragment" -> StringField,
    "refr_medium" -> StringField,
    "refr_source" -> StringField,
    "refr_term" -> StringField,
    "mkt_medium" -> StringField,
    "mkt_source" -> StringField,
    "mkt_term" -> StringField,
    "mkt_content" -> StringField,
    "mkt_campaign" -> StringField,
    "contexts" -> ContextsField,
    "se_category" -> StringField,
    "se_action" -> StringField,
    "se_label" -> StringField,
    "se_property" -> StringField,
    "se_value" -> StringField,
    "unstruct_event" -> UnstructField,
    "tr_orderid" -> StringField,
    "tr_affiliation" -> StringField,
    "tr_total" -> DoubleField,
    "tr_tax" -> DoubleField,
    "tr_shipping" -> DoubleField,
    "tr_city" -> StringField,
    "tr_state" -> StringField,
    "tr_country" -> StringField,
    "ti_orderid" -> StringField,
    "ti_sku" -> StringField,
    "ti_name" -> StringField,
    "ti_category" -> StringField,
    "ti_price" -> DoubleField,
    "ti_quantity" -> IntField,
    "pp_xoffset_min" -> IntField,
    "pp_xoffset_max" -> IntField,
    "pp_yoffset_min" -> IntField,
    "pp_yoffset_max" -> IntField,
    "useragent" -> StringField,
    "br_name" -> StringField,
    "br_family" -> StringField,
    "br_version" -> StringField,
    "br_type" -> StringField,
    "br_renderengine" -> StringField,
    "br_lang" -> StringField,
    "br_features_pdf" -> BoolField,
    "br_features_flash" -> BoolField,
    "br_features_java" -> BoolField,
    "br_features_director" -> BoolField,
    "br_features_quicktime" -> BoolField,
    "br_features_realplayer" -> BoolField,
    "br_features_windowsmedia" -> BoolField,
    "br_features_gears" -> BoolField,
    "br_features_silverlight" -> BoolField,
    "br_cookies" -> BoolField,
    "br_colordepth" -> StringField,
    "br_viewwidth" -> IntField,
    "br_viewheight" -> IntField,
    "os_name" -> StringField,
    "os_family" -> StringField,
    "os_manufacturer" -> StringField,
    "os_timezone" -> StringField,
    "dvce_type" -> StringField,
    "dvce_ismobile" -> BoolField,
    "dvce_screenwidth" -> IntField,
    "dvce_screenheight" -> IntField,
    "doc_charset" -> StringField,
    "doc_width" -> IntField,
    "doc_height" -> IntField,
    "tr_currency" -> StringField,
    "tr_total_base" -> DoubleField,
    "tr_tax_base" -> DoubleField,
    "tr_shipping_base" -> DoubleField,
    "ti_currency" -> StringField,
    "ti_price_base" -> DoubleField,
    "base_currency" -> StringField,
    "geo_timezone" -> StringField,
    "mkt_clickid" -> StringField,
    "mkt_network" -> StringField,
    "etl_tags" -> StringField,
    "dvce_sent_tstamp" -> TstampField,
    "refr_domain_userid" -> StringField,
    "refr_device_tstamp" -> TstampField,
    "derived_contexts" -> ContextsField,
    "domain_sessionid" -> StringField,
    "derived_tstamp" -> TstampField,
    "event_vendor" -> StringField,
    "event_name" -> StringField,
    "event_format" -> StringField,
    "event_version" -> StringField,
    "true_tstamp" -> TstampField
  )

  private object GeopointIndexes {
    val latitude = 22
    val longitude = 23
  }

  /**
   * Converts a timestamp to ISO 8601 format
   *
   * @param tstamp Timestamp of the form YYYY-MM-DD hh:mm:ss
   * @return ISO 8601 timestamp
   */
  private def reformatTstamp (tstamp: String): String = tstamp.replaceAll(" ", "T") + "Z"

  /**
   * Converts "0" to false and "1" to true
   *
   * @param key The field name
   * @param value The field value - should be "0" or "1"
   * @return Validated JObject
   */
  private def handleBooleanField(key: String, value: String): ValidationNel[String, JObject] =
    value match {
      case "1" => JObject(key -> JBool(true)).successNel
      case "0" => JObject(key -> JBool(false)).successNel
      case _   => "Value [%s] is not valid for field [%s]: expected 0 or 1".format(value, key).failNel
    }

}

/**
 * Class to convert successfully enriched events to EmitterInputs
 *
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 */
class SnowplowElasticsearchTransformer(documentIndex: String, documentType: String) extends ITransformer[ValidatedRecord, EmitterInput] {

  private lazy val log = LoggerFactory.getLogger(getClass())

  /**
   * Convert the value of a field to a JValue based on the name of the field
   *
   * @param fieldInformation ((field name, field-to-JObject conversion function), field value)
   * @return JObject representing a single field in the JSON
   */
  private def converter(fieldInformation: ((String, TsvToJsonConverter), String)): ValidationNel[String, JObject] = {
    val ((fieldName, fieldConversionFunction), fieldValue) = fieldInformation
    if (fieldValue.isEmpty) {
      JObject(fieldName -> JNull).successNel
    } else {
      try {
        fieldConversionFunction(fieldName, fieldValue)
      } catch {
        case e @ (_ : IllegalArgumentException | _: JsonParseException) =>
          "Value [%s] is not valid for field [%s]: %s".format(fieldValue, fieldName, e.getMessage).failNel
      }

    }
  }

  /**
   * Converts an aray of field values to a JSON whose keys are the field names
   *
   * @param event Array of values for the event
   * @return ValidatedRecord containing JSON for the event and the event_id (if it exists)
   */
  def jsonifyGoodEvent(event: Array[String]): ValidationNel[String, JsonRecord] = {
    val fields = SnowplowElasticsearchTransformer.fields

    if (event.size != fields.size) {
      log.warn(s"Expected ${fields.size} fields, received ${event.size} fields. This may be caused by using an outdated version of Snowplow Kinesis Enrich.")
    }

    if (event.size <= SnowplowElasticsearchTransformer.GeopointIndexes.latitude.max(SnowplowElasticsearchTransformer.GeopointIndexes.longitude)) {
      s"Event contained only ${event.size} tab-separated fields".failNel
    } else {

      val geoLocation: JObject = {
        val latitude = event(SnowplowElasticsearchTransformer.GeopointIndexes.latitude)
        val longitude = event(SnowplowElasticsearchTransformer.GeopointIndexes.longitude)
        if (latitude.size > 0 && longitude.size > 0) {
          JObject("geo_location" -> JString(s"$latitude,$longitude"))
        } else {
          JObject()
        }
      }
      val validatedJObjects: List[ValidationNel[String, JObject]] = fields.zip(event.toList).map(converter)
      val switched: ValidationNel[String, List[JObject]] = validatedJObjects.sequenceU
      switched.map( x => {
        val j = x.fold(geoLocation)((x,y) => y ~ x)
        JsonRecord(compact(render(j)), extractEventId(j))
      })
    }
  }

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of an enriched event string
   * @return ValidatedRecord for the event
   */
  override def toClass(record: Record): ValidatedRecord = {
    val recordString = new String(record.getData.array, UTF_8)

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

  /**
   * Extract the event_id field from an event JSON for use as a document ID
   *
   * @param json
   * @return Option boxing event_id
   */
  private def extractEventId(json: JValue): Option[String] = {
    json \ "event_id" match {
      case JString(eid) => eid.some
      case _ => None
    }
  }

}
