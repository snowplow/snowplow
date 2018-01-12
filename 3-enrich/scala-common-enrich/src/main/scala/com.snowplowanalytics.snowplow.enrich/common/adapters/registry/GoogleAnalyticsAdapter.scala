/*
 * Copyright (c) 2018-2018 Snowplow Analytics Ltd. All rights reserved.
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
  private val VendorName = "GoogleAnalytics"
  private val GaVendor = "com.google.analytics"
  private val Vendor = s"$GaVendor.measurement-protocol"
  private val ProtocolVersion = "v1"
  private val Protocol = s"$Vendor-$ProtocolVersion"
  private val Format = "jsonschema"
  private val SchemaVersion = "1-0-0"

  private val PageViewHitType = "pageview"

  // models a translation between measurement protocol fields and the fields in Iglu schemas
  type Translation = Function1[String, Validation[String, FieldType]]

  /**
   * Case class holding the name of the field in the Iglu schemas as well as the necessary
   * translation between the original MP string and the typed value
   * @param fieldName name of the field in the Iglu schemas
   * @param translation going from the raw string in the MP payload to the typed data fit to be
   * incorporated in a schema
   */
  final case class KVTranslation(fieldName: String, translation: Translation)

  /**
   * Case class representing measurement protocol schema data
   * @param schemaKey key of the Iglu schema
   * @param translationTable mapping of measurement protocol field names to field names in Iglu
   * schemas
   */
  final case class MPData(schemaKey: SchemaKey, translationTable: Map[String, KVTranslation])

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
  private val idTranslation: (String => KVTranslation) = (fieldName: String) =>
    KVTranslation(fieldName, (value: String) => StringType(value).success)
  private val intTranslation: (String => KVTranslation) = (fieldName: String) =>
    KVTranslation(fieldName, stringToJInteger(fieldName, _: String).map(i => IntType(i.toInt)))
  private val twoDecimalsTranslation: (String => KVTranslation) = (fieldName: String) =>
    KVTranslation(fieldName, stringToTwoDecimals(fieldName, _: String).map(DoubleType))
  private val doubleTranslation: (String => KVTranslation) = (fieldName: String) =>
    KVTranslation(fieldName, stringToDouble(fieldName, _: String).map(DoubleType))
  private val booleanTranslation: (String => KVTranslation) = (fieldName: String) =>
    KVTranslation(fieldName, stringToBoolean(fieldName, _: String).map(BooleanType))

  // unstruct event mappings
  private val unstructEventData: Map[String, MPData] = Map(
    "pageview" -> MPData(
      SchemaKey(Vendor, "page_view", Format, SchemaVersion),
      Map(
        "dl" -> idTranslation("documentLocationUrl"),
        "dh" -> idTranslation("documentHostName"),
        "dp" -> idTranslation("documentPath"),
        "dt" -> idTranslation("documentTitle")
      )
    ),
    "screenview" -> MPData(SchemaKey(Vendor, "screen_view", Format, SchemaVersion),
      Map("cd" -> idTranslation("screenName"))),
    "event" -> MPData(
      SchemaKey(Vendor, "event", Format, SchemaVersion),
      Map(
        "ec" -> idTranslation("category"),
        "ea" -> idTranslation("action"),
        "el" -> idTranslation("label"),
        "ev" -> intTranslation("value")
      )
    ),
    "transaction" -> MPData(
      SchemaKey(Vendor, "transaction", Format, SchemaVersion),
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
      SchemaKey(Vendor, "item", Format, SchemaVersion),
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
      SchemaKey(Vendor, "social", Format, SchemaVersion),
      Map(
        "sn" -> idTranslation("network"),
        "sa" -> idTranslation("action"),
        "st" -> idTranslation("actionTarget")
      )
    ),
    "exception" -> MPData(
      SchemaKey(Vendor, "exception", Format, SchemaVersion),
      Map(
        "exd" -> idTranslation("description"),
        "exf" -> booleanTranslation("isFatal")
      )
    ),
    "timing" -> MPData(
      SchemaKey(Vendor, "timing", Format, SchemaVersion),
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
  private val contextData: Map[SchemaKey, Map[String, KVTranslation]] = {
    // pageview can be a context too
    val ct = unstructEventData(PageViewHitType) :: List(
      MPData(SchemaKey(GaVendor, "undocumented", Format, SchemaVersion),
        List("a", "jid", "gjid").map(e => e -> idTranslation(e)).toMap),
      MPData(SchemaKey(GaVendor, "private", Format, SchemaVersion),
        (List("_v", "_u", "_gid").map(e => e -> idTranslation(e.tail)) ++
          List("_s", "_r").map(e => e -> intTranslation(e.tail))).toMap),
      MPData(SchemaKey(Vendor, "general", Format, SchemaVersion),
        Map(
          "v"   -> idTranslation("protocolVersion"),
          "tid" -> idTranslation("trackingId"),
          "aip" -> booleanTranslation("anonymizeIp"),
          "ds"  -> idTranslation("dataSource"),
          "qt"  -> intTranslation("queueTime"),
          "z"   -> idTranslation("cacheBuster")
        )
      ),
      MPData(SchemaKey(Vendor, "user", Format, SchemaVersion),
        Map("cid" -> idTranslation("clientId"), "uid" -> idTranslation("userId"))),
      MPData(SchemaKey(Vendor, "session", Format, SchemaVersion),
        Map(
          "sc"    -> idTranslation("sessionControl"),
          "uip"   -> idTranslation("ipOverride"),
          "ua"    -> idTranslation("userAgentOverride"),
          "geoid" -> idTranslation("geographicalOverride")
        )
      ),
      MPData(SchemaKey(Vendor, "traffic_source", Format, SchemaVersion),
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
      MPData(SchemaKey(Vendor, "system_info", Format, SchemaVersion),
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
      MPData(SchemaKey(Vendor, "link", Format, SchemaVersion),
        Map("linkid" -> idTranslation("id"))),
      MPData(SchemaKey(Vendor, "app", Format, SchemaVersion),
        Map(
          "an"   -> idTranslation("name"),
          "aid"  -> idTranslation("id"),
          "av"   -> idTranslation("version"),
          "aiid" -> idTranslation("installerId")
        )
      ),
      MPData(SchemaKey(Vendor, "product_action", Format, SchemaVersion),
        Map(
          "pa"  -> idTranslation("productAction"),
          "pal" -> idTranslation("productActionList"),
          "cos" -> intTranslation("checkoutStep"),
          "col" -> idTranslation("checkoutStepOption")
        )
      ),
      MPData(SchemaKey(Vendor, "content_experiment", Format, SchemaVersion),
        Map("xid" -> idTranslation("id"), "xvar" -> idTranslation("variant"))),
      MPData(SchemaKey(Vendor, "hit", Format, SchemaVersion),
        Map("t" -> idTranslation("type"), "ni" -> booleanTranslation("nonInteractionHit"))),
      MPData(SchemaKey(Vendor, "promotion_action", Format, SchemaVersion),
        Map("promoa" -> idTranslation("promotionAction")))
    )
    ct.map(d => d.schemaKey -> d.translationTable).toMap
  }

  // layer of indirection linking flat context fields to schemas
  private val fieldToSchemaMap: Map[String, SchemaKey] = contextData
    .flatMap { case (schema, trTable) => trTable.keys.map(_ -> schema) }

  // IF indicates that the value is in the field name
  private val valueInFieldNameIndicator = "IF"
  // composite context mappings
  private val compositeContextData: List[MPData] = List(
    MPData(SchemaKey(Vendor, "product", Format, SchemaVersion),
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
    MPData(SchemaKey(Vendor, "product_custom_dimension", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}prcd" -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cd"   -> intTranslation("dimensionIndex"),
        "prcd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "product_custom_metric", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}prcm" -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cm"   -> intTranslation("metricIndex"),
        "prcm"                              -> intTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "product_impression_list", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}il" -> intTranslation("index"),
        "ilnm"                            -> idTranslation("name")
      )
    ),
    MPData(SchemaKey(Vendor, "product_impression", Format, SchemaVersion),
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
    MPData(SchemaKey(Vendor, "product_impression_custom_dimension", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}ilpicd" -> intTranslation("listIndex"),
        s"${valueInFieldNameIndicator}picd"   -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cd"     -> intTranslation("customDimensionIndex"),
        "ilpicd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "product_impression_custom_metric", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}ilpicm" -> intTranslation("listIndex"),
        s"${valueInFieldNameIndicator}picm"   -> intTranslation("productIndex"),
        s"${valueInFieldNameIndicator}cm"     -> intTranslation("customMetricIndex"),
        "ilpicm"                              -> intTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "promotion", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}promo" -> intTranslation("index"),
        "promoid"                            -> idTranslation("id"),
        "promonm"                            -> idTranslation("name"),
        "promocr"                            -> idTranslation("creative"),
        "promops"                            -> idTranslation("position")
      )
    ),
    MPData(SchemaKey(Vendor, "custom_dimension", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}cd" -> intTranslation("index"),
        "cd"                              -> idTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "custom_metric", Format, SchemaVersion),
      Map(
        s"${valueInFieldNameIndicator}cm" -> intTranslation("index"),
        "cm"                              -> doubleTranslation("value")
      )
    ),
    MPData(SchemaKey(Vendor, "content_group", Format, SchemaVersion),
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
  private val nrCompFieldsPerSchema: Map[SchemaKey, Int] =
    compositeContextData.map { d =>
      d.schemaKey -> d.translationTable.count(_._1.startsWith(valueInFieldNameIndicator))
    }.toMap

  // direct mappings between the measurement protocol and the snowplow tracker protocol
  private val directMappings: (String => Map[String, String]) = (hitType: String) => Map(
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
      case None => s"Request body is empty: no $VendorName events to process".failNel
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
      case None => s"No $VendorName t parameter provided: cannot determine hit type".failNel
      case Some(hitType) =>
        // direct mappings
        val mappings = translatePayload(params, directMappings(hitType))
        val translationTable = unstructEventData.get(hitType).map(_.translationTable)
          .toSuccess(s"No matching $VendorName hit type for hit type $hitType".wrapNel)
        val schemaVal = lookupSchema(
          hitType.some, VendorName, unstructEventData.mapValues(_.schemaKey.toSchemaUri))
        val simpleContexts = buildContexts(params, contextData, fieldToSchemaMap)
        val compositeContexts = buildCompositeContexts(params, compositeContextData,
          nrCompFieldsPerSchema, valueInFieldNameIndicator).validation.toValidationNel

        (translationTable |@| schemaVal |@| simpleContexts |@| compositeContexts) {
          (trTable, schema, contexts, compContexts) =>
            val contextJsons = (contexts.toList ++ compContexts)
              .collect {
                // an unnecessary pageview context might have been built so we need to remove it
                case (s, d) if hitType != PageViewHitType ||
                  s != unstructEventData(PageViewHitType).schemaKey => buildJson(s.toSchemaUri, d)
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
                    Map("e" -> "ue", "ue_pr" -> unstructEvent, "tv" -> Protocol, "p" -> "srv"),
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
    translationTable: Map[String, KVTranslation]
  ): ValidationNel[String, Map[String, FieldType]] =
    originalParams.foldLeft(Map.empty[String, ValidationNel[String, FieldType]]) {
      case (m, (fieldName, value)) =>
        translationTable
          .get(fieldName)
          .map { case KVTranslation(newName, translation) =>
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
    referenceTable: Map[SchemaKey, Map[String, KVTranslation]],
    fieldToSchemaMap: Map[String, SchemaKey]
  ): ValidationNel[String, Map[SchemaKey, Map[String, FieldType]]] =
    originalParams.foldLeft(Map.empty[SchemaKey, Map[String, ValidationNel[String, FieldType]]]) {
      case (m, (fieldName, value)) =>
        fieldToSchemaMap.get(fieldName).map { schema =>
          // this is safe when fieldToSchemaMap is built from referenceTable
          val KVTranslation(newName, translation) = referenceTable(schema)(fieldName)
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
   * @param indicator indicator used to determine if a key-value has been extracted from the
   * composite field name
   * @return a map containing the composite contexts keyed by schema
   */
  private def buildCompositeContexts(
    originalParams: Map[String, String],
    referenceTable: List[MPData],
    nrCompFieldsPerSchema: Map[SchemaKey, Int],
    indicator: String
  ): \/[String, List[(SchemaKey, Map[String, FieldType])]] =
    for {
      // composite params have digits in their key or are cu
      composite  <- originalParams
        .filterKeys(k => k.exists(_.isDigit) || k == "cu")
        .right
      brokenDown <- composite
        .toList
        .map {
          case (k, v) if k == "cu" => Map(k -> v).right
          case (k, v) => breakDownCompField(k, v, indicator)
        }
        .sequenceU
      grouped     = brokenDown.flatten.groupBy(_._1).mapValues(_.map(_._2))
      translated <-
        grouped.foldLeft(Map.empty[SchemaKey, Map[String, \/[String, Seq[FieldType]]]]) {
          case (m, (fieldName, values)) =>
            val additions = referenceTable
              .filter(_.translationTable.contains(fieldName))
              .map { d =>
                // this is safe because of the filter above
                val KVTranslation(newName, translation) = d.translationTable(fieldName)
                val trTable = m.getOrElse(d.schemaKey, Map.empty) +
                  (newName -> values.map(v => translation(v).disjunction).sequenceU)
                d.schemaKey -> trTable
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
   *   - the composite field name ends with a sub field name
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

  private val compositeFieldRegex =
    ("(" + List("pr", "promo", "il", "cd", "cm", "cg").mkString("|") + ")" +
      """(\d+)([a-zA-Z]*)(\d*)([a-zA-Z]*)(\d*)$""").r
  /**
   * Breaks down measurement protocol composite fields in a pair of list of strings.
   * e.g. abc12def45 becomes (List("abc", "def"), List("12", "45"))
   * @param fieldName raw composite field name
   * @return the break down of the field or a failure if it couldn't be parsed
   */
  private[registry] def breakDownCompField(
      fieldName: String): \/[String, (List[String], List[String])] = fieldName match {
    case compositeFieldRegex(grps@_*) => splitEvenOdd(grps.toList.filter(_.nonEmpty)).right
    case s if s.isEmpty => "Cannot parse empty composite field name".left
    case _ => (s"Cannot parse field name $fieldName, " +
      s"it doesn't conform to the expected composite field regex: $compositeFieldRegex").left
  }

  /** Splits a list in two based on the oddness or evenness of their indices */
  private def splitEvenOdd[T](list: List[T]): (List[T], List[T]) = {
    def go(l: List[T], even: List[T], odd: List[T]): (List[T], List[T], List[T]) = l match {
      case h1 :: h2 :: t => go(t, h1 :: even, h2 :: odd)
      case h :: Nil => (Nil, h :: even, odd)
      case Nil => (Nil, even, odd)
    }
    val res = go(list, Nil, Nil)
    (res._2.reverse, res._3.reverse)
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
