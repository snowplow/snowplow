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

  case class HitTypeData(schemaUri: String, translationTable: Map[String, String])

  val hitTypeData = Map(
    "pageview" -> HitTypeData(
      SchemaKey(vendor, "page_view", format, schemaVersion).toSchemaUri,
      Map(
        "dl" -> "documentLocationURL",
        "dh" -> "documentHostName",
        "dp" -> "documentPath",
        "dt" -> "documentTitle"
      )
    ),
    "screenview" -> HitTypeData(
      SchemaKey(vendor, "screen_view", format, schemaVersion).toSchemaUri, Map("cd" -> "name")),
    "event" -> HitTypeData(
      SchemaKey(vendor, "event", format, schemaVersion).toSchemaUri,
      Map(
        "ec" -> "category",
        "ea" -> "action",
        "el" -> "label",
        "ev" -> "value"
      )
    ),
    "transaction" -> HitTypeData(
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
    "item" -> HitTypeData(
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
    "social" -> HitTypeData(
      SchemaKey(vendor, "social", format, schemaVersion).toSchemaUri,
      Map(
        "sn" -> "network",
        "sa" -> "action",
        "st" -> "actionTarget"
      )
    ),
    "exception" -> HitTypeData(
      SchemaKey(vendor, "exception", format, schemaVersion).toSchemaUri,
      Map(
        "exd" -> "description",
        "exf" -> "isFatal"
      )
    ),
    "timing" -> HitTypeData(
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
   *
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
            unstructEventParams = translateParams(params, trTable, hitType)
          } yield NonEmptyList(RawEvent(
            api         = payload.api,
            parameters  = toUnstructEventParams(
                            protocol, unstructEventParams, schema, buildFormatter(), "srv"),
            contentType = payload.contentType,
            source      = payload.source,
            context     = payload.context
          ))
      }
    }
  }

  private def translateParams(
    originalParams: Map[String, String],
    translationTable: Map[String, String],
    hitType: String
  ): Map[String, String] =
    originalParams.foldLeft(Map("hitType" -> hitType)) { (m, e) =>
      translationTable.get(e._1).map(newName => m + (newName -> e._2)).getOrElse(m)
    }
}