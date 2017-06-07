/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics
package snowplow.enrich.common
package adapters
package registry

// Java
import java.net.URI
import org.apache.http.client.utils.URLEncodedUtils

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{Resolver, SchemaKey}

// This project
import loaders.CollectorPayload
import utils.ConversionUtils._

/**
 * Transforms a collector payload which conforms to a known version of the Google Analytics
 * protocol into raw events.
 */
object GoogleAnalyticsAdapter extends Adapter {

  // for failure messages
  private val vendorName = "GoogleAnalytics"
  private val gaVendor = "com.google.analytics"
  private val vendor = s"$gaVendor.measurement-protocol"
  private val protocolVersion = "v1"
  private val protocol = s"$vendor-$protocolVersion"
  private val format = "jsonschema"
  private val schemaVersion = "1-0-0"

  private val pageViewHitType = "pageview"

  // models a translation between measurement protocol fields and the fields in Iglu schemas
  type Translation = (Function1[String, Validation[String, FieldType]], String)
  /**
   * Case class representing measurement protocol schema data
   * @param schemaUri uri of the Iglu schema
   * @param translationTable mapping of measurement protocol field names to field names in Iglu
   * schemas
   */
  case class MPData(schemaUri: String, translationTable: Map[String, Translation])

  // class hierarchy defined to type the measurement protocol payload
  sealed trait FieldType
  final case class StringType(s: String) extends FieldType
  final case class IntType(i: Int) extends FieldType
  final case class DoubleType(d: Double) extends FieldType
  final case class BooleanType(b: Boolean) extends FieldType
  implicit val fieldTypeJson4s: FieldType => JValue = (f: FieldType) =>
    f match {
      case StringType(s)  => JString(s)
      case IntType(i)     => JInt(i)
      case DoubleType(f)  => JDouble(f)
      case BooleanType(b) => JBool(b)
    }

  // translations between string and the needed types in the measurement protocol Iglu schemas
  private val idTranslation: (String => Translation) = (fieldName: String) =>
    ((value: String) => StringType(value).success, fieldName)
  private val intTranslation: (String => Translation) = (fieldName: String) =>
    (stringToJInteger(fieldName, _: String).map(i => IntType(i.toInt)), fieldName)
  private val twoDecimalsTranslation: (String => Translation) = (fieldName: String) =>
    (stringToTwoDecimals(fieldName, _: String).map(DoubleType), fieldName)
  private val doubleTranslation: (String => Translation) = (fieldName: String) =>
    (stringToDouble(fieldName, _: String).map(DoubleType), fieldName)
  private val booleanTranslation: (String => Translation) = (fieldName: String) =>
    (stringToBoolean(fieldName, _: String).map(BooleanType), fieldName)

  // unstruct event mappings
  private val unstructEventData = Map(
    "pageview" -> MPData(
      SchemaKey(vendor, "page_view", format, schemaVersion).toSchemaUri,
      Map(
        "dl" -> idTranslation("documentLocationUrl"),
        "dh" -> idTranslation("documentHostName"),
        "dp" -> idTranslation("documentPath"),
        "dt" -> idTranslation("documentTitle")
      )
    ),
    "screenview" -> MPData(SchemaKey(vendor, "screen_view", format, schemaVersion).toSchemaUri,
      Map("cd" -> idTranslation("screenName"))),
    "event" -> MPData(
      SchemaKey(vendor, "event", format, schemaVersion).toSchemaUri,
      Map(
        "ec" -> idTranslation("category"),
        "ea" -> idTranslation("action"),
        "el" -> idTranslation("label"),
        "ev" -> intTranslation("value")
      )
    ),
    "transaction" -> MPData(
      SchemaKey(vendor, "transaction", format, schemaVersion).toSchemaUri,
      Map(
        "ti"  -> idTranslation("id"),
        "ta"  -> idTranslation("affiliation"),
        "tr"  -> twoDecimalsTranslation("revenue"),
        "ts"  -> twoDecimalsTranslation("shipping"),
        "tt"  -> twoDecimalsTranslation("tax"),
        "tcc" -> idTranslation("couponCode"),
        "cu"  -> idTranslation("currencyCode")
      )
    ),
    "item" -> MPData(
      SchemaKey(vendor, "item", format, schemaVersion).toSchemaUri,
      Map(
        "ti" -> idTranslation("transactionId"),
        "in" -> idTranslation("name"),
        "ip" -> twoDecimalsTranslation("price"),
        "iq" -> intTranslation("quantity"),
        "ic" -> idTranslation("code"),
        "iv" -> idTranslation("category"),
        "cu" -> idTranslation("currencyCode")
      )
    ),
    "social" -> MPData(
      SchemaKey(vendor, "social", format, schemaVersion).toSchemaUri,
      Map(
        "sn" -> idTranslation("network"),
        "sa" -> idTranslation("action"),
        "st" -> idTranslation("actionTarget")
      )
    ),
    "exception" -> MPData(
      SchemaKey(vendor, "exception", format, schemaVersion).toSchemaUri,
      Map(
        "exd" -> idTranslation("description"),
        "exf" -> booleanTranslation("isFatal")
      )
    ),
    "timing" -> MPData(
      SchemaKey(vendor, "timing", format, schemaVersion).toSchemaUri,
      Map(
        "utc" -> idTranslation("userTimingCategory"),
        "utv" -> idTranslation("userTimingVariableName"),
        "utt" -> intTranslation("userTimingTime"),
        "utl" -> idTranslation("userTimingLabel"),
        "plt" -> intTranslation("pageLoadTime"),
        "dns" -> intTranslation("dnsTime"),
        "pdt" -> intTranslation("pageDownloadTime"),
        "rrt" -> intTranslation("redirectResponseTime"),
        "tcp" -> intTranslation("tcpConnectTime"),
        "srt" -> intTranslation("serverResponseTime"),
        "dit" -> intTranslation("domInteractiveTime"),
        "clt" -> intTranslation("contentLoadTime")
      )
    )
  )

  // flat context mappings
  private val contextData = {
    // pageview can be a context too
    val ct = unstructEventData(pageViewHitType) :: List(
      MPData(SchemaKey(gaVendor, "undocumented", format, schemaVersion).toSchemaUri,
        List("a", "jid", "gjid").map(e => e -> idTranslation(e)).toMap),
      MPData(SchemaKey(gaVendor, "private", format, schemaVersion).toSchemaUri,
        (List("_v", "_u", "_gid").map(e => e -> idTranslation(e.tail)) ++
          List("_s", "_r").map(e => e -> intTranslation(e.tail))).toMap),
      MPData(SchemaKey(vendor, "general", format, schemaVersion).toSchemaUri,
        Map(
          "v"   -> idTranslation("protocolVersion"),
          "tid" -> idTranslation("trackingId"),
          "aip" -> booleanTranslation("anonymizeIp"),
          "ds"  -> idTranslation("dataSource"),
          "qt"  -> intTranslation("queueTime"),
          "z"   -> idTranslation("cacheBuster")
        )
      ),
      MPData(SchemaKey(vendor, "user", format, schemaVersion).toSchemaUri,
        Map("cid" -> idTranslation("clientId"), "uid" -> idTranslation("userId"))),
      MPData(SchemaKey(vendor, "session", format, schemaVersion).toSchemaUri,
        Map(
          "sc"    -> idTranslation("sessionControl"),
          "uip"   -> idTranslation("ipOverride"),
          "ua"    -> idTranslation("userAgentOverride"),
          "geoid" -> idTranslation("geographicalOverride")
        )
      ),
      MPData(SchemaKey(vendor, "traffic_source", format, schemaVersion).toSchemaUri,
        Map(
          "dr"    -> idTranslation("documentReferrer"),
          "cn"    -> idTranslation("campaignName"),
          "cs"    -> idTranslation("campaignSource"),
          "cm"    -> idTranslation("campaignMedium"),
          "ck"    -> idTranslation("campaignKeyword"),
          "cc"    -> idTranslation("campaignContent"),
          "ci"    -> idTranslation("campaignId"),
          "gclid" -> idTranslation("googleAdwordsId"),
          "dclid" -> idTranslation("googleDisplayAdsId")
        )
      ),
      MPData(SchemaKey(vendor, "system_info", format, schemaVersion).toSchemaUri,
        Map(
          "sr" -> idTranslation("screenResolution"),
          "vp" -> idTranslation("viewportSize"),
          "de" -> idTranslation("documentEncoding"),
          "sd" -> idTranslation("screenColors"),
          "ul" -> idTranslation("userLanguage"),
          "je" -> booleanTranslation("javaEnabled"),
          "fl" -> idTranslation("flashVersion")
        )
      ),
      MPData(SchemaKey(vendor, "link", format, schemaVersion).toSchemaUri,
        Map("linkid" -> idTranslation("id"))),
      MPData(SchemaKey(vendor, "app", format, schemaVersion).toSchemaUri,
        Map(
          "an"   -> idTranslation("name"),
          "aid"  -> idTranslation("id"),
          "av"   -> idTranslation("version"),
          "aiid" -> idTranslation("installerId")
        )
      ),
      MPData(SchemaKey(vendor, "product_action", format, schemaVersion).toSchemaUri,
        Map(
          "pa"  -> idTranslation("productAction"),
          "pal" -> idTranslation("productActionList"),
          "cos" -> intTranslation("checkoutStep"),
          "col" -> idTranslation("checkoutStepOption")
        )
      ),
      MPData(SchemaKey(vendor, "content_experiment", format, schemaVersion).toSchemaUri,
        Map("xid" -> idTranslation("id"), "xvar" -> idTranslation("variant"))),
      MPData(SchemaKey(vendor, "hit", format, schemaVersion).toSchemaUri,
        Map("t" -> idTranslation("type"), "ni" -> booleanTranslation("nonInteractionHit"))),
      MPData(SchemaKey(vendor, "promotion_action", format, schemaVersion).toSchemaUri,
        Map("promoa" -> idTranslation("promotionAction")))
    )
    ct.map(d => d.schemaUri -> d.translationTable).toMap
  }

  // layer of indirection linking flat context fields to schemas
  private val fieldToSchemaMap = contextData
    .flatMap { case (schema, trTable) => trTable.keys.map(_ -> schema) }

  // IF indicates that the value is in the field name
  private val valueInFieldNameIndicator = "IF"
  // composite context mappings
  private val compositeContextData = List(
    MPData(SchemaKey(vendor, "product", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}pr" -> intTranslation("index"),
        "prid"                            -> idTranslation("sku"),
        "prnm"                            -> idTranslation("name"),
        "prbr"                            -> idTranslation("brand"),
        "prca"                            -> idTranslation("category"),
        "prva"                            -> idTranslation("variant"),
        "prpr"                            -> twoDecimalsTranslation("price"),
        "prqt"                            -> intTranslation("quantity"),
        "prcc"                            -> idTranslation("couponCode"),
        "prps"                            -> intTranslation("position"),
        "cu"                              -> idTranslation("currencyCode")
      )
    ),
    MPData(SchemaKey(vendor, "product_custom_dimension", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}prcd" -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cd"   -> intTranslation("dimensionIndex"),
        "prcd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "product_custom_metric", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}prcm" -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cm"   -> intTranslation("metricIndex"),
        "prcm"                              -> intTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "product_impression_list", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}il" -> intTranslation("index"),
        "ilnm"                            -> idTranslation("name")
      )
    ),
    MPData(SchemaKey(vendor, "product_impression", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}ilpi" -> intTranslation("listIndex"),
        s"${valueInFieldNameIndicator}pi"   -> intTranslation("productIndex"),
        "ilpiid"                            -> idTranslation("sku"),
        "ilpinm"                            -> idTranslation("name"),
        "ilpibr"                            -> idTranslation("brand"),
        "ilpica"                            -> idTranslation("category"),
        "ilpiva"                            -> idTranslation("variant"),
        "ilpips"                            -> intTranslation("position"),
        "ilpipr"                            -> twoDecimalsTranslation("price"),
        "cu"                                -> idTranslation("currencyCode")
      )
    ),
    MPData(SchemaKey(vendor, "product_impression_custom_dimension", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}ilpicd" -> intTranslation("listIndex"),
        s"${valueInFieldNameIndicator}picd"   -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cd"     -> intTranslation("customDimensionIndex"),
        "ilpicd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "product_impression_custom_metric", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}ilpicm" -> intTranslation("listIndex"),
        s"${valueInFieldNameIndicator}picm"   -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cm"     -> intTranslation("customMetricIndex"),
        "ilpicm"                              -> intTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "promotion", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}promo" -> intTranslation("index"),
        "promoid"                            -> idTranslation("id"),
        "promonm"                            -> idTranslation("name"),
        "promocr"                            -> idTranslation("creative"),
        "promops"                            -> idTranslation("position")
      )
    ),
    MPData(SchemaKey(vendor, "custom_dimension", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}cd" -> intTranslation("index"),
        "cd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "custom_metric", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}cm" -> intTranslation("index"),
        "cm"                              -> doubleTranslation("value")
      )
    ),
    MPData(SchemaKey(vendor, "content_group", format, schemaVersion).toSchemaUri,
      Map(
        s"${valueInFieldNameIndicator}cg" -> intTranslation("index"),
        "cg"                              -> idTranslation("value")
      )
    )
  )

  // mechanism used to filter out composite contexts that might have been built unnecessarily
  // e.g. if the field cd is in the payload it can be a screen name or a custom dimension
  // it can only be a custom dimension if the field is in the form cd12 which maps to two fields:
  // IFcd -> 12 and cd -> value, as a result it can be a custom dimension if there are more fields
  // than there are IF fields in the constructed map
  // This map holds the number of IF fields in the composite context mappings to ease the check
  // described above
  private val nrCompFieldsPerSchema =
    compositeContextData.map { d =>
      d.schemaUri -> d.translationTable.count(_._1.startsWith(valueInFieldNameIndicator))
    }.toMap

  // This is used to find the composite fields in the original payload
  // cu is here because it can be part of a schema containing composite fields despite not being
  // composite itself
  private val compositeFieldPrefixes = List("pr", "cu", "il", "cd", "cm", "cg")

  // direct mappings between the measurement protocol and the snowplow tracker protocol
  private val directMappings = (hitType: String) => Map(
    "uip" -> "ip",
    "dr"  -> "refr",
    "de"  -> "cs",
    "sd"  -> "cd",
    "ul"  -> "lang",
    "je"  -> "f_java",
    "dl"  -> "url",
    "dt"  -> "page",
    "ti"  -> (if (hitType == "transaction") "tr_id" else "ti_id"),
    "ta"  -> "tr_af",
    "tr"  -> "tr_tt",
    "ts"  -> "tr_sh",
    "tt"  -> "tr_tx",
    "in"  -> "ti_nm",
    "ip"  -> "ti_pr",
    "iq"  -> "ti_qu",
    "ic"  -> "ti_sk",
    "iv"  -> "ti_ca",
    "cu"  -> (if (hitType == "transaction") "tr_cu" else "ti_cu"),
    "ua"  -> "ua"
  )

  /**
   * Converts a CollectorPayload instance of (possibly multiple) Google Analytics payloads into raw
   * events.
   * @param payload The CollectorPaylod containing one or more raw Google Analytics payloads as
   * collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (for {
      body <- payload.body
      rawEvents <- body.lines.map(parsePayload(_, payload)).toList.toNel
    } yield rawEvents) match {
      case Some(rawEvents) => rawEvents.sequenceU
      case None => s"Request body is empty: no $vendorName events to process".failNel
    }

  /**
   * Parses one Google Analytics payload.
   * @param bodyPart part of the payload's body corresponding to one Google Analytics payload
   * @param payload original CollectorPayload
   * @return a Validation boxing either a RawEvent or a NEL of Failure Strings
   */
  private def parsePayload(bodyPart: String, payload: CollectorPayload): ValidationNel[String, RawEvent] = {
    val params = toMap(URLEncodedUtils.parse(URI.create(s"http://localhost/?$bodyPart"), "UTF-8").toList)
    params.get("t") match {
      case None => s"No $vendorName t parameter provided: cannot determine hit type".failNel
      case Some(hitType) =>
        // direct mappings
        val mappings = translatePayload(params, directMappings(hitType))

        (
          unstructEventData.get(hitType).map(_.translationTable)
            .toSuccess(s"No matching $vendorName hit type for hit type $hitType".wrapNel)  |@|
          lookupSchema(hitType.some, vendorName, unstructEventData.mapValues(_.schemaUri)) |@|
          buildContexts(params, contextData, fieldToSchemaMap)                             |@|
          buildCompositeContexts(params, compositeContextData, nrCompFieldsPerSchema,
            compositeFieldPrefixes, valueInFieldNameIndicator).validation.toValidationNel
        ) { (trTable, schema, contexts, compositeContexts) =>
          val contextJsons = (contexts.toList ++ compositeContexts)
            .collect {
              // an unnecessary pageview context might have been built so we need to remove it
              case (s, d) if hitType != pageViewHitType ||
                s != unstructEventData(pageViewHitType).schemaUri => buildJson(s, d)
            }
          val contextParam =
            if (contextJsons.isEmpty) Map.empty
            else Map("co" -> compact(toContexts(contextJsons)))

          translatePayload(params, trTable)
            .map { e =>
              val unstructEvent = compact(toUnstructEvent(buildJson(schema, e)))
              RawEvent(
                api         = payload.api,
                parameters  = contextParam ++ mappings ++
                  Map("e" -> "ue", "ue_pr" -> unstructEvent, "tv" -> protocol, "p" -> "srv"),
                contentType = payload.contentType,
                source      = payload.source,
                context     = payload.context
              )
            }
        }.flatMap(identity)
    }
  }

  /**
   * Translates a payload according to a translation table.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between original params and the wanted format
   * @return a translated params
   */
  private def translatePayload(
    originalParams: Map[String, String],
    translationTable: Map[String, Translation]
  ): ValidationNel[String, Map[String, FieldType]] =
    originalParams.foldLeft(Map.empty[String, ValidationNel[String, FieldType]]) {
      case (m, (fieldName, value)) =>
        translationTable
          .get(fieldName)
          .map { case (translation, newName) =>
            m + (newName -> translation(value).toValidationNel)
          }
          .getOrElse(m)
    }.sequenceU

  /**
   * Translates a payload according to a translation table.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between original params and the wanted format
   * @return a translated params
   */
  private def translatePayload(
    originalParams: Map[String, String],
    translationTable: Map[String, String]
  ): Map[String, String] =
    originalParams.foldLeft(Map.empty[String, String]) {
      case (m, (fieldName, value)) =>
        translationTable
          .get(fieldName)
          .map(newName => m + (newName -> value))
          .getOrElse(m)
    }

  /**
   * Discovers the contexts in the payload in linear time (size of originalParams).
   * @param originalParams original payload in key-value format
   * @param referenceTable map of context schemas and their associated translations
   * @param fieldToSchemaMap reverse indirection from referenceTable linking fields with the MP
   * nomenclature to schemas
   * @return a map containing the discovered contexts keyed by schema
   */
  private def buildContexts(
    originalParams: Map[String, String],
    referenceTable: Map[String, Map[String, Translation]],
    fieldToSchemaMap: Map[String, String]
  ): ValidationNel[String, Map[String, Map[String, FieldType]]] =
    originalParams.foldLeft(Map.empty[String, Map[String, ValidationNel[String, FieldType]]]) {
      case (m, (fieldName, value)) =>
        fieldToSchemaMap.get(fieldName).map { schema =>
          // this is safe when fieldToSchemaMap is built from referenceTable
          val (translation, newName) = referenceTable(schema)(fieldName)
          val trTable = m.getOrElse(schema, Map.empty) +
            (newName -> translation(value).toValidationNel)
          m + (schema -> trTable)
        }
        .getOrElse(m)
    }
    .map { case (k, v) => (k -> v.sequenceU) }
    .sequenceU

  /**
   * Builds the contexts containing composite fields in quadratic time
   * (nr of composite fields * avg size for each of them)
   * @param originalParams original payload in key-value format
   * @param referenceTable list of context schemas containing composite fields and their
   * associated translations
   * @param nrCompFieldsPerSchema map containing the number of field values in the composite field
   * name. Used to filter out contexts that might have been erroneously built.
   * @param compFieldPrefixes list of prefixes that are composite fields. Used to filter the
   * original payload
   * @param indicator indicator used to determine if a key-value has been extracted from the
   * composite field name
   * @return a map containing the composite contexts keyed by schema
   */
  private def buildCompositeContexts(
    originalParams: Map[String, String],
    referenceTable: List[MPData],
    nrCompFieldsPerSchema: Map[String, Int],
    compFieldPrefixes: List[String],
    indicator: String
  ): \/[String, List[(String, Map[String, FieldType])]] =
    for {
      // composite params have their two first chars in the list of prefixes or are cu
      // this is done to avoid conflicts between cd custom dimension and cd screen name
      composite  <- originalParams
        .filterKeys(k => compFieldPrefixes.contains(k.take(2)) && (k.length > 2 || k == "cu"))
        .right
      brokenDown <- composite
        .toList
        .map { case (k, v) => breakDownCompField(k, v, indicator) }
        .sequenceU
      grouped     = brokenDown.flatten.groupBy(_._1).mapValues(_.map(_._2))
      translated <-
        grouped.foldLeft(Map.empty[String, Map[String, \/[String, Seq[FieldType]]]]) {
          case (m, (fieldName, values)) =>
            val additions = referenceTable
              .filter(_.translationTable.contains(fieldName))
              .map { d =>
                // this is safe because of the filter above
                val (translation, newName) = d.translationTable(fieldName)
                val trTable = m.getOrElse(d.schemaUri, Map.empty) +
                  (newName -> values.map(v => translation(v).disjunction).sequenceU)
                d.schemaUri -> trTable
              }.toMap
            m ++ additions
        }
        .map { case (k, v) => (k -> v.sequenceU) }
        .sequenceU
      transposed  = translated.mapValues { m =>
        val values = transpose(m.values.map(_.toList).toList)
        values.map(m.keys zip _).map(_.toMap)
      }
      // we need to filter out composite contexts which might have been built unnecessarily
      // eg due to ${indicator}pr being in 3 different schemas
      filtered    = transposed
        .map { case (k, vs) => k -> vs.filter(_.size > nrCompFieldsPerSchema(k)) }
        .filter(_._2.nonEmpty)
      flattened   = filtered.toList.flatMap { case (k, vs) => vs.map(k -> _) }
    } yield flattened

  /**
   * Breaks down measurement protocol composite fields into a small deterministic payload.
   * Two cases are possible:
   *   - the composite field name ends with a value
   *      e.g. pr12 -> val in this case the payload becomes Map(indicatorpr -> 12, pr -> val)
   *    - the composite field name ends with a sub field name
   *      e.g. pr12id -> val in this case the payload becomes Map(indicatorpr -> 12, prid -> val)
   * @param fieldName raw composite field name
   * @param value of the composite field
   * @param indicator string used to notify that this extracted key-value pair was from the
   * original composite field name
   * @return a mini payload extracted from the composite field or a failure
   */
  private[registry] def breakDownCompField(
    fieldName: String,
    value: String,
    indicator: String
  ): \/[String, Map[String, String]] = for {
    brokenDown <- breakDownCompField(fieldName)
    (strs, ints) = brokenDown
    m <- if (strs.length == ints.length) {
        (strs.scanRight("")(_ + _).init.map(indicator + _) zip ints).toMap.right
      } else if (strs.length == ints.length + 1) {
        (strs.init.scanRight("")(_ + _).init.map(indicator + _) zip ints).toMap.right
      } else {
        // can't happen without changing breakDownCompField(fieldName)
        s"Cannot parse field name $fieldName, unexpected number of values inside".left
      }
    r = m + (strs.reduce(_ + _) -> value)
  } yield r

  /**
   * Breaks down measurement protocol composite fields in a pair of list of strings.
   * e.g. abc12def45 becomes (List("abc", "def"), List("12", "45"))
   * @param fieldName raw composite field name
   * @return the break down of the field or a failure if it couldn't be parsed
   */
  private[registry] def breakDownCompField(
      fieldName: String): \/[String, (List[String], List[String])] =
    if (fieldName.headOption.isEmpty || fieldName.head.isDigit) {
      s"Malformed composite field name: $fieldName".left
    } else {
      val (ss, is, _) = fieldName.foldLeft((List.empty[String], List.empty[String], false)) {
        case ((strs, ints, lastWasString), char) =>
          if (char.isDigit) {
            if (lastWasString) (strs, char.toString :: ints, false)
            else (strs, ints.head + char :: ints.tail, false)
          } else {
            if (!lastWasString) (char.toString :: strs, ints, true)
            else (strs.head + char :: strs.tail, ints, true)
          }
      }
      (ss.reverse, is.reverse).right
    }

  /** Transposes a list of lists, does not need to be rectangular unlike the stdlib's version. */
  private def transpose[T](l: List[List[T]]): List[List[T]] =
    l.flatMap(_.headOption) match {
      case Nil => Nil
      case head => head :: transpose(l.collect { case _ :: tail => tail })
    }

  private def buildJson(schema: String, fields: Map[String, FieldType]): JValue =
    ("schema" -> schema) ~ ("data" -> fields)
}