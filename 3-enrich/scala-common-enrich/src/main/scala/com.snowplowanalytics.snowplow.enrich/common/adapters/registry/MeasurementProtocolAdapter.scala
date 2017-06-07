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

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.{Resolver, SchemaKey}

// This project
import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the Google Analytics
 * protocol into raw events.
 */
object MeasurementProtocolAdapter extends Adapter {

  // for failure messages
  private val vendorName = "MeasurementProtocol"
  private val vendor = "com.google.analytics.measurement-protocol"
  private val protocolVersion = "v1"
  private val protocol = s"$vendor-$protocolVersion"
  private val format = "jsonschema"
  private val schemaVersion = "1-0-0"

  case class MPData(schemaUri: String, translationTable: Map[String, String])

  val hitTypeData = Map(
    "pageview" -> MPData(
      SchemaKey(vendor, "page_view", format, schemaVersion).toSchemaUri,
      Map(
        "dl" -> "documentLocationURL",
        "dh" -> "documentHostName",
        "dp" -> "documentPath",
        "dt" -> "documentTitle"
      )
    ),
    "screenview" -> MPData(
      SchemaKey(vendor, "screen_view", format, schemaVersion).toSchemaUri, Map("cd" -> "name")),
    "event" -> MPData(
      SchemaKey(vendor, "event", format, schemaVersion).toSchemaUri,
      Map(
        "ec" -> "category",
        "ea" -> "action",
        "el" -> "label",
        "ev" -> "value"
      )
    ),
    "transaction" -> MPData(
      SchemaKey(vendor, "transaction", format, schemaVersion).toSchemaUri,
      Map(
        "ti"  -> "id",
        "ta"  -> "affiliation",
        "tr"  -> "revenue",
        "ts"  -> "shipping",
        "tt"  -> "tax",
        "tcc" -> "couponCode",
        "cu"  -> "currencyCode"
      )
    ),
    "item" -> MPData(
      SchemaKey(vendor, "item", format, schemaVersion).toSchemaUri,
      Map(
        "ti" -> "id",
        "in" -> "name",
        "ip" -> "price",
        "iq" -> "quantity",
        "ic" -> "code",
        "iv" -> "category",
        "cu" -> "currencyCode"
      )
    ),
    "social" -> MPData(
      SchemaKey(vendor, "social", format, schemaVersion).toSchemaUri,
      Map(
        "sn" -> "network",
        "sa" -> "action",
        "st" -> "actionTarget"
      )
    ),
    "exception" -> MPData(
      SchemaKey(vendor, "exception", format, schemaVersion).toSchemaUri,
      Map(
        "exd" -> "description",
        "exf" -> "isFatal"
      )
    ),
    "timing" -> MPData(
      SchemaKey(vendor, "timing", format, schemaVersion).toSchemaUri,
      Map(
        "utc" -> "userTimingCategory",
        "utv" -> "userTimingVariableName",
        "utt" -> "userTimingTime",
        "utl" -> "userTimingLabel",
        "plt" -> "pageLoadTime",
        "dns" -> "dnsTime",
        "pdt" -> "pageDownloadTime",
        "rrt" -> "redirectResponseTime",
        "tcp" -> "tcpConnectTime",
        "srt" -> "serverResponseTime",
        "dit" -> "domInteractiveTime",
        "clt" -> "contentLoadTime"
      )
    )
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * @param payload The CollectorPaylod containing one or more raw events as collected by
   * a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      s"Querystring is empty: no $vendorName event to process".failNel
    } else {
      params.get("t") match {
        case None => s"No $vendorName t parameter provided: cannot determine hit type".failNel
        case Some(hitType) =>
          for {
            trTable <- hitTypeData.get(hitType).map(_.translationTable)
              .toSuccess(NonEmptyList(s"No matching $vendorName hit type for hit type $hitType"))
            schema <- lookupSchema(hitType.some, vendorName, hitTypeData.mapValues(_.schemaUri))
            unstructEvent = buildUnstructEvent(params, trTable, hitType)
          } yield NonEmptyList(RawEvent(
            api         = payload.api,
            parameters  = toUnstructEventParams(
                            protocol, unstructEvent, schema, buildFormatter(), "srv"),
            contentType = payload.contentType,
            source      = payload.source,
            context     = payload.context
          ))
      }
    }
  }

  /**
   * Builds an unstruct event by finding the appropriate fields in the original payload and
   * translating them.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between MP and iglu schema for the specific hit type
   * @param hitType hit type of this unstruct event
   * @return an unstruct event in key-value format
   */
  private def buildUnstructEvent(
    originalParams: Map[String, String],
    translationTable: Map[String, String],
    hitType: String
  ): Map[String, String] =
    originalParams.foldLeft(Map("hitType" -> hitType)) { (m, e) =>
      translationTable.get(e._1).map(newName => m + (newName -> e._2)).getOrElse(m)
    }

  /**
   * Discovers the contexts in the payload in linear time (size of originalParams).
   * @param originalParams original payload in key-value format
   * @param referenceTable list of context schemas and their associated translation
   * @param fieldToSchemaMap reverse indirection from referenceTable linking fields to schemas
   * @return a map containing the discovered contexts keyed by schema
   */
  private def buildContexts(
    originalParams: Map[String, String],
    referenceTable: List[MPData],
    fieldToSchemaMap: Map[String, String]
  ): Map[String, Map[String, String]] = {
    val refTable = referenceTable.map(d => d.schemaUri -> d.translationTable).toMap
    originalParams.foldLeft(Map.empty[String, Map[String, String]]) { (m, e) =>
      fieldToSchemaMap.get(e._1).map { s =>
        // this is safe when fieldToSchemaMap is built from referenceTable
        val translated = refTable(s)(e._1)
        val trTable = m.getOrElse(s, Map.empty) + (translated -> e._2)
        m + (s -> trTable)
      }
      .getOrElse(m)
    }
  }
}