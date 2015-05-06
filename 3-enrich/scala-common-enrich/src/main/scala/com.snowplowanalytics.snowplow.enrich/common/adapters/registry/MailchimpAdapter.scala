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

// Java
import java.net.URI
import org.apache.http.client.utils.URLEncodedUtils

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

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
import iglu.client.validation.ValidatableJsonMethods._

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Mailchimp Tracking webhook
 * into raw events.
 */
object MailchimpAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "MailChimp"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Tracker version for an Mailchimp Tracking webhook
  private val TrackerVersion = "com.mailchimp-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "subscribe"   -> SchemaKey("com.mailchimp", "subscribe", "jsonschema", "1-0-0").toSchemaUri,
    "unsubscribe" -> SchemaKey("com.mailchimp", "unsubscribe", "jsonschema", "1-0-0").toSchemaUri,
    "campaign"    -> SchemaKey("com.mailchimp", "campaign_sending_status", "jsonschema", "1-0-0").toSchemaUri,
    "cleaned"     -> SchemaKey("com.mailchimp", "cleaned_email", "jsonschema", "1-0-0").toSchemaUri,
    "upemail"     -> SchemaKey("com.mailchimp", "email_address_change", "jsonschema", "1-0-0").toSchemaUri,
    "profile"     -> SchemaKey("com.mailchimp", "profile_update", "jsonschema", "1-0-0").toSchemaUri
  )

  // Datetime format used by MailChimp (as we will need to massage)
  private val MailchimpDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  // Formatter Function to convert RawEventParameters into a merged Json Object
  private val MailchimpFormatter: FormatterFunc = {
    (parameters: RawEventParameters) => mergeJFields(toJFields(parameters))
  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * An Mailchimp Tracking payload only contains a single event.
   * We expect the name parameter to be 1 of 6 options otherwise
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
    (payload.body, payload.contentType) match {
      case (None, _)                          => s"Request body is empty: no ${VendorName} event to process".failNel
      case (_, None)                          => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body), _)                    => {

        val params = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
        params.get("type") match {
          case None => s"No ${VendorName} type parameter provided: cannot determine event type".failNel
          case Some(eventType) => {

            val allParams = toMap(payload.querystring) ++ reformatParameters(params)
            for {
              schema <- lookupSchema(eventType.some, VendorName, EventSchemaMap)
            } yield {
              NonEmptyList(RawEvent(
                api          = payload.api,
                parameters   = toUnstructEventParams(TrackerVersion, allParams, schema, MailchimpFormatter, "srv"),
                contentType  = payload.contentType,
                source       = payload.source,
                context      = payload.context
              ))
            }
          }
        }
      }
    }

  /**
   * Generates a List of JFields from the raw event parameters.
   * 
   * @param parameters The Map of all the parameters
   *        for this raw event
   * @return a (possibly-empty) List of JFields, where each
   *         JField represents an entry from teh incoming Map
   */
  private[registry] def toJFields(parameters: RawEventParameters): List[JField] = {
    for {
      (k, v) <- parameters.toList
    } yield toNestedJField(toKeys(k), v)
  }

  /**
   * Returns a NonEmptyList of nested keys from a String representing
   * a field from a URI-encoded POST body.
   * 
   * @param formKey The key String that (may) need to be split based on
   *        the supplied regexp
   * @return the key or keys as a NonEmptyList of Strings
   */
  private[registry] def toKeys(formKey: String): NonEmptyList[String] = {
    val keys = formKey.split("\\]?(\\[|\\])").toList
    NonEmptyList(keys(0), keys.tail: _*) // Safe only because split() never produces an empty Array
  }

  /**
   * Recursively generates a correct JField,
   * working through the supplied NEL of keys.
   *
   * @param keys The NEL of keys remaining to
   *        nest into our JObject
   * @param value The value we are going to
   *        finally insert when we run out
   *        of keys
   * @return a JField built from the list of key(s) and
   *         a value
   */
  private[registry] def toNestedJField(keys: NonEmptyList[String], value: String): JField =
    keys.toList match {
      case head :: second :: tail => JField(head, toNestedJField(NonEmptyList(second, tail: _*), value))
      case head :: Nil            => JField(head, JString(value))
    }

  /**
   * Merges a List of possibly overlapping nested JFields together,
   * thus:
   *
   * val a: JField = ("data", JField("nested", JField("more-nested", JField("str", "hi"))))
   * val b: JField = ("data", JField("nested", JField("more-nested", JField("num", 42))))
   * => JObject(List((data,JObject(List((nested,JObject(List((more-nested,JObject(List((str,JString(hi)), (num,JInt(42)))))))))))))
   *    aka {"data":{"nested":{"more-nested":{"str":"hi","num":42}}}}
   *
   * @param jfields A (possibly-empty) list of JFields which
   *        need to be merged together
   * @return a fully merged JObject from the List of JFields provided,
   *         or a JObject(Nil) if the List was empty
   */
  private[registry] def mergeJFields(jfields: List[JField]): JObject =
    jfields match {
      case x :: xs => xs.foldLeft(JObject(x))(_ merge JObject(_))
      case Nil => JObject(Nil)
    }

  /**
   * Reformats the date-time stored in the fired_at parameter
   * (if found) so that it can pass JSON Schema date-time validation.
   *
   * @param parameters The parameters to be checked for fixing
   * @return the event parameters, either with a fixed date-time
   *         for fired_at if that key was found, or else the original
   *         parameters
   */
  private[registry] def reformatParameters(parameters: RawEventParameters): RawEventParameters =
    parameters.get("fired_at") match {
      case Some(firedAt) => parameters.updated("fired_at", JU.toJsonSchemaDateTime(firedAt, MailchimpDateTimeFormat))
      case None          => parameters
    }
}
