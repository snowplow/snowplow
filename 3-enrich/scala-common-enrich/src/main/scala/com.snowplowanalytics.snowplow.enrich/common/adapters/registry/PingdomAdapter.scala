/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import org.apache.http.NameValuePair

// Scala
import scala.util.matching.Regex

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Pingdom Tracking webhook
 * into raw events.
 */
object PingdomAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Pingdom"

  // Tracker version for an Pingdom Tracking webhook
  private val TrackerVersion = "com.pingdom-v1"

  // Regex for extracting data from querystring values
  private val PingdomValueRegex = """\(u'(.+)',\)""".r

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "assign"          -> SchemaKey("com.pingdom", "incident_assign", "jsonschema", "1-0-0").toSchemaUri,
    "notify_of_close" -> SchemaKey("com.pingdom", "incident_notify_of_close", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Pingdom Tracking payload only contains a single event.
   * We expect the name parameter to be one of two types otherwise
   * we have an unsupported event type.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = 
    (payload.querystring) match {
      case (Nil) => s"Request querystring is empty: no Pingdom event to process".failNel
      case (qs)  => {

        reformatMapParams(qs) match {
          case Failure(NonEmptyList(f)) => f.failNel
          case Success(s) => {

            s.get("message") match {
              case None => s"The querystring does not contain any event to process".failNel
              case Some(event) => {

                for {
                  parsedEvent <- parseJsonSafe(event)
                  schema <- {
                    val eventOpt = (parsedEvent \ "action").extractOpt[String]
                    lookupSchema(eventOpt, VendorName, EventSchemaMap)
                  }
                } yield {
                  val qsParams = s - "message"
                  NonEmptyList(RawEvent(
                    api          = payload.api,
                    parameters   = toUnstructEventParams(TrackerVersion, qsParams, schema, parsedEvent, "srv"),
                    contentType  = payload.contentType,
                    source       = payload.source,
                    context      = payload.context
                  ))
                }
              }
            }
          }
        }
      }
    }

  /**
   * As Pingdom wraps each value in the querystring within: (u'[content]',)
   * We need to remove these wrappers from every value before we can use them.
   * example: p -> (u'app',) becomes p -> app
   * The expected behaviour is that every value will be in this form, if 
   * Pingdom changes this we will not be able to process any events.
   *
   * @param params Is a list of name-value pairs from the querystring of 
   *        the Pingdom payload
   * @return a Map of name-value pairs which has been validated as all 
   *         passing the regex extraction or return a NonEmptyList of Failures 
   *         if any could not pass the regex.
   */
  private[registry] def reformatMapParams(params: List[NameValuePair]): Validated[Map[String,String]] = {
    val formatted = params.map { 
      value => {
        (value.getName, value.getValue) match {
          case (k, PingdomValueRegex(v)) => (k -> v).successNel
          case (k, v) => s"Name value pair [$k -> $v]: did not match regex".failNel
        }
      }
    }

    val successes: List[(String, String)] = 
      for {
        Success(s) <- formatted 
      } yield s

    val failures: List[String] = 
      for {
        Failure(NonEmptyList(f)) <- formatted 
      } yield f

    (successes, failures) match {
      case (s :: ss,     Nil) => (s :: ss).toMap.successNel // No Failures collected.
      case (s :: ss, f :: fs) => NonEmptyList(f, fs: _*).fail // Some Failures, return only those.
      case (Nil,           _) => "List of events is empty (should never happen, not catching empty list properly)".failNel
    }
  }

  /**
   * Attempts to parse a json string into a JValue
   * example: {"p":"app"} becomes JObject(List((p,JString(app))))
   * 
   * @param jsonStr The string we want to parse into a JValue
   * @return a Validated JValue or a NonEmptyList Failure 
   *         containing a JsonParseException
   */
  private[registry] def parseJsonSafe(jsonStr: String): Validated[JValue] =
    try {
      parse(jsonStr).successNel
    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString)
        s"Pingdom event failed to parse into JSON: [$exception]".failNel
      } 
    }
}
