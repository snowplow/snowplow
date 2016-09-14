/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.net.URI
import org.apache.http.client.utils.URLEncodedUtils

// Scala
import scala.collection.JavaConversions._

// ScalaZ
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// This project
import loaders.CollectorPayload
import utils.{
  JsonUtils => JU
}

/*
 * Transforms a collector payload which conforms to
 * a known version of the Mixpanel webhook into
 * raw events.
 */
object MixpanelAdapter extends Adapter {

  // Vendor name for failure message
  private val VendorName = "Mixpanel"

  // Tracker Version for Mixpanel webhook
  private val TrackerVersion = "com.mixpanel-v1"

  // Expected content-type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "users" -> SchemaKey("com.mixpanel", "users", "jsonschema", "1-0-0").toSchemaUri
  )
  
  /**
    * Returns a list of events from the request
    * body of a Mixpanel Event. Each event will
    * be formatted as an individual JSON of type
    * JValue.
    *
    * @param body The urlencoded string
    *        from the Mixpanel POST request
    * @return a list of validated raw events
    */


  private def payloadBodyToEvents(body: String, eventOpt: String, payload: CollectorPayload): List[Validated[RawEvent]] = {
    try {
      val parsed = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
      val users = parsed.get("users")
      users match {
        case Some(list) =>
            val events = parse(list)
            for {
              (event, index) <- events.children.zipWithIndex
            } yield {
              for {
                schema <- lookupSchema(Some(eventOpt), VendorName, index, EventSchemaMap)
              } yield {
                val qsParams = toMap(payload.querystring)
                val formattedEvent = event map {
                  j =>  j transformField { case (key, value) => (key.replaceAll(" ", ""), value) }
              }
              RawEvent(
                api = payload.api,
                parameters = toUnstructEventParams(TrackerVersion,
                  qsParams,
                  schema,
                  formattedEvent,
                  "srv"),
                contentType = payload.contentType,
                source = payload.source,
                context = payload.context
              )
            }
          }
        case _ =>  List(s"${VendorName} request body does not have 'users' as a key: invalid event to process".failNel)
      }
    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString).orNull
        List(s"$VendorName event failed to parse into JSON: [$exception]".failNel)
      }
    }
  }


/* Converts a CollectorPayload instance into raw events.
 *
 * @param payload The CollectorPayload containing one or more
 *        raw events as collected by a Snowplow collector
 * @param resolver (implicit) The Iglu resolver used for
 *        schema lookup and validation
 * @return a Validation boxing either a NEL of RawEvents on
 *         Success, or a NEL of Failure Strings
 */

  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, None) => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body), _) => {
        val querystring = toMap(payload.querystring)
        val events = querystring.get("schema") match {
          case None => List(s"No ${VendorName} schema type provided in querystring: cannot determine event type".failNel)
          case Some(schema) => payloadBodyToEvents(body, schema, payload)
        }
        rawEventsListProcessor(events)
      }
    }
  }
}