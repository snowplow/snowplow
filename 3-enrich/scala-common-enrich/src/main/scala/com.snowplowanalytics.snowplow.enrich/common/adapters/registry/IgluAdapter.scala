/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow
package enrich
package common
package adapters
package registry

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.fasterxml.jackson.core.JsonParseException

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which provides a set
 * of name-value pairs on a GET querystring, along
 * with a &schema=[[iglu schema uri]] parameter to
 * attribute the name-value pairs to an
 * Iglu-compatible self-describing JSON.
 */
object IgluAdapter extends Adapter {

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "com.snowplowanalytics.iglu-v1"
  private val VendorName = "Iglu"

  // Create a simple formatter function
  private val IgluFormatter: FormatterFunc = buildFormatter() // For defaults

    /**
     *
     * Converts a payload into a list of validated events
     * Expects a valid json - returns a single failure if one is not present
     *
     * @param body json payload as POST'd by a webhook
     * @param payload the rest of the payload details
     * @return a list of validated events, successes will be the corresponding raw events
     *         failures will contain a non empty list of the reason(s) for the particular event failing
     */
    private def payloadBodyToEvents(body: String, payload: CollectorPayload): List[Validated[RawEvent]] = {

      val parsed = parseJsonSafe(body) match {
        case Success(jsonBody) => jsonBody
      }

      val queryString = toMap(payload.querystring)
      val schema = queryString.get("schema") match {
        case Some(schemaUri) => schemaUri
      }

      if (parsed.children.isEmpty) {
        return List(s"$VendorName event failed json sanity check: has no events".failNel)
      }

      List(RawEvent(
        api          = payload.api,
        parameters   = toUnstructEventParams(TrackerVersion,
                        queryString,
                        schema,
                         parsed,
                         "srv"),
        contentType  = payload.contentType,
        source       = payload.source,
        context      = payload.context
      ).success)
    }


  /**
   * Converts a CollectorPayload instance into raw events.
   * Currently we only support a single event Iglu-compatible
   * self-describing event passed in on the querystring.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {

    val params = toMap(payload.querystring)

    // check if params are empty
    if (params.isEmpty) {
      "Querystring is empty: no Iglu-compatible event to process".failNel
    } else {
      params.get("schema") match {
        case None => "Querystring does not contain schema parameter: not an Iglu-compatible self-describing event".failNel
        case Some(schemaUri) => SchemaKey.parse(schemaUri) match {
          case Failure(procMsg) => procMsg.getMessage.failNel
          case Success(_)       => (payload.body, payload.contentType) match {
            case(None, _) => // if it's a GET request
                  NonEmptyList(RawEvent(
                    api          = payload.api,
                    parameters   = toUnstructEventParams(TrackerVersion, (params - "schema"), schemaUri, IgluFormatter, "app"),
                    contentType  = payload.contentType,
                    source       = payload.source,
                    context      = payload.context
                    )).success
            case(_, None) => "Content type has not been specified".failNel // POST request with no content type
            case (Some(body), Some(contentType)) => payload.contentType match { // POST request with body and content type
              case Some("application/json") =>
                  val events = payloadBodyToEvents(body, payload)
                  rawEventsListProcessor(events)
              case _ => "Content type not supported".failNel
            }
          }
        }
      }
    }
  }
}
