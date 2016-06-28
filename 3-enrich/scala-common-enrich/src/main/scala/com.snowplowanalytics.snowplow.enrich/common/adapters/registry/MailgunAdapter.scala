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
import org.apache.http.NameValuePair
import javax.crypto.spec.SecretKeySpec

// Scala
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the StatusGator Tracking webhook
 * into raw events.
 */
object MailgunAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Mailgun"

  // Tracker version for an Mailgun Tracking webhook
  private val TrackerVersion = "com.mailgun-v1"

  // Expected content type for a request body
  private val ContentTypes = List("application/x-www-form-urlencoded", "multipart/form-data")
  private val ContentTypesStr = ContentTypes.mkString(" or ")

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "bounced"      -> SchemaKey("com.mailgun", "message_bounced", "jsonschema", "1-0-0").toSchemaUri,
    "clicked"      -> SchemaKey("com.mailgun", "message_clicked", "jsonschema", "1-0-0").toSchemaUri,
    "complained"   -> SchemaKey("com.mailgun", "message_complained", "jsonschema", "1-0-0").toSchemaUri,
    "delivered"    -> SchemaKey("com.mailgun", "message_delivered", "jsonschema", "1-0-0").toSchemaUri,
    "dropped"      -> SchemaKey("com.mailgun", "message_dropped", "jsonschema", "1-0-0").toSchemaUri,
    "opened"       -> SchemaKey("com.mailgun", "message_opened", "jsonschema", "1-0-0").toSchemaUri,
    "unsubscribed" -> SchemaKey("com.mailgun", "recipient_unsubscribed", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * A Mailgun Tracking payload contains one single event
   * in the body of the payload, stored within a HTTP encoded
   * string.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    (payload.body, payload.contentType) match {
      case (None, _)                                                => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, None)                                                => s"Request body provided but content type empty, expected ${ContentTypesStr} for ${VendorName}".failNel
      case (_, Some(ct)) if !ContentTypes.exists(ct.startsWith(_))  => s"Content type of ${ct} provided, expected ${ContentTypesStr} for ${VendorName}".failNel
      case (Some(body), _) if (body.isEmpty)                        => s"${VendorName} event body is empty: nothing to process".failNel
      case (Some(body), Some(ct))                                   => {
        val params = toMap(payload.querystring)
        try {
          val bodyMap = getBoundary(ct)
                          .map(parseMultipartForm(body, _))
                          .getOrElse(toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList))
          bodyMap.get("event") match {
            case None            => s"No ${VendorName} event parameter provided: cannot determine event type".failNel
            case Some(eventType) => {
              (payloadBodyToEvent(bodyMap)) match {
                case (Failure(err)) => err.failNel
                case (Success(event)) => {
                  lookupSchema(eventType.some, VendorName, EventSchemaMap) match {
                    case Failure(str)  => str.fail
                    case Success(schemaUri) => {
                      NonEmptyList(RawEvent(
                        api         = payload.api,
                        parameters  = toUnstructEventParams(TrackerVersion, params, schemaUri, event, "srv"),
                        contentType = payload.contentType,
                        source      = payload.source,
                        context     = payload.context
                      )).success
                    }
                  } 
                }
              }
            }
          }
        } catch { 
          case NonFatal(e) => {
            val exception = JU.stripInstanceEtc(e.getMessage).orNull
            s"${VendorName}Adapter could not parse body: [${exception}]".failNel
          }
        }
      }
    }
  }
  /**
  * Returns the boundary parameter for a message of media type multipart/form-data
  * (https://www.ietf.org/rfc/rfc2616.txt and https://www.ietf.org/rfc/rfc2046.txt)
  *
  * @param contentType Header field of the form "multipart/form-data; boundary=353d603f-eede-4b49-97ac-724fbc54ea3c"
  * @return boundary Option[String]
  */
  private val boundaryRegex = """multipart/form-data.*?boundary=(?:")?([\S ]{0,69})(?: )*(?:")?$""".r
  private def getBoundary(contentType: String): Option[String] = contentType match {
    case boundaryRegex(boundaryString) => Some(boundaryString)
    case _ => None
  }
  /**
   * Rudimentary parsing the form fields of a multipart/form-data into a Map[String, String]
   * other fields will be discarded (see https://www.ietf.org/rfc/rfc1867.txt and https://www.ietf.org/rfc/rfc2046.txt).
   * This parser will only take into account part headers of content-disposition type form-data
   * and only the parameter name e.g.
   * Content-Disposition: form-data; anything="notllokingintothis"; name="key"
   *
   * value
   *
   * @param body The body of the message
   * @param boundary String that separates the body parts
   * @return a map of the form fields and their values (other fields are dropped)
   */
  private val formDataRegex = """(?sm).*Content-Disposition:\s*form-data\s*;[ \S\t]*?name="([^"]+)"[ \S\t]*$.*?(?<=^[ \t\S]*$)^\s*(.*?)(?:\s*)\z""".r
  private def parseMultipartForm(body: String, boundary: String): Map[String, String] =
    body
      .split(s"--$boundary")
      .flatMap( _ match {
        case formDataRegex(k,v) => Some((k,v))
        case _ => None
      })
      .toMap

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(bodyMap: Map[String, String]): Validation[String, JObject] = {
    (bodyMap.get("timestamp"), bodyMap.get("token"), bodyMap.get("signature")) match {
      case (None, _, _) => s"${VendorName} event data missing 'timestamp'".fail
      case (_, None, _) => s"${VendorName} event data missing 'token'".fail
      case (_, _, None) => s"${VendorName} event data missing 'signature'".fail
      case (Some(timestamp), Some(token), Some(signature)) => {
        try {
          val json = compact(render(bodyMap))
          val event = parse(json)
          event match {
            case obj: JObject => obj.success
            case _            => s"${VendorName} event wrong type: [%s]".format(event.getClass).fail
          }
        } catch {
          case e: JsonParseException => {
              val exception = JU.stripInstanceEtc(e.toString).orNull
              s"${VendorName} event string failed to parse into JSON: [${exception}]".fail
          }
          case e: Exception => {
            val exception = JU.stripInstanceEtc(e.toString).orNull
            s"${VendorName} incorrect event string : [${exception}]".fail
          }
        } 
      }
    }
  }
}
