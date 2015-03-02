/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

// Apache URLEncodedUtils
import org.apache.http.NameValuePair

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

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

trait Adapter {

  // The Iglu schema URI for a Snowplow unstructured event
  private val UnstructEvent = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri

  // Signature for a Formatter function
  type FormatterFunc = (RawEventParameters) => JObject

  // Needed for json4s default extraction formats
  implicit val formats = DefaultFormats

  // The encoding type to be used
  val EventEncType = "UTF-8"

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents

  /**
   * Converts a NonEmptyList of name:value
   * pairs into a Map.
   *
   * @param parameters A NonEmptyList of name:value pairs
   * @return the name:value pairs in Map form
   */
  // TODO: can this become private?
  protected[registry] def toMap(parameters: List[NameValuePair]): Map[String, String] =
    parameters.map(p => (p.getName -> p.getValue)).toList.toMap

  /**
   * Convenience function to build a simple formatter
   * of RawEventParameters.
   *
   * @param bools A List of keys whose values should be
   *        processed as boolean-like Strings
   * @param ints A List of keys whose values should be
   *        processed as integer-like Strings
   * @param dates If Some, a NEL of keys whose values should
   *        be treated as date-time-like Strings, which will
   *        require processing from the specified format
   * @return a formatter function which converts
   *         RawEventParameters into a cleaned JObject
   */
  protected[registry] def buildFormatter(bools: List[String] = Nil, ints: List[String] = Nil,
    dateTimes: JU.DateTimeFields = None): FormatterFunc = {

    (parameters: RawEventParameters) => for {
      p <- parameters.toList
    } yield JU.toJField(p._1, p._2, bools, ints, dateTimes)
  }

  /**
   * Fabricates a Snowplow unstructured event from
   * the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event
   * must contain e, p and tv parameters, so we
   * make sure to set those.
   *
   * @param tracker The name and version of this
   *        tracker
   * @param parameters The raw-event parameters
   *        we will nest into the unstructured event
   * @param schema The schema key which defines this
   *        unstructured event as a String
   * @param formatter A function to take the raw event
   *        parameters and turn them into a correctly
   *        formatted JObject that should pass JSON
   *        Schema validation
   * @param platform The default platform to assign
   *         the event to
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  protected[registry] def toUnstructEventParams(tracker: String, parameters: RawEventParameters, schema: String,
    formatter: FormatterFunc, platform: String): RawEventParameters = {

    val params = formatter(parameters - ("nuid", "aid", "cv", "p"))

    val json = compact {
      ("schema" -> UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
        ("data"   -> params)
      ))
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> parameters.getOrElse("p", platform), // Required field
      "ue_pr" -> json) ++
    parameters.filterKeys(Set("nuid", "aid", "cv"))
  }

  /**
   * Fabricates a Snowplow unstructured event from
   * the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event
   * must contain e, p and tv parameters, so we
   * make sure to set those.
   *
   * @param tracker The name and version of this
   *        tracker
   * @param qsParams The query-string parameters
   *        we will nest into the unstructured event
   * @param schema The schema key which defines this
   *        unstructured event as a String
   * @param eventJson The event which we will nest
   *        into the unstructured event
   * @param platform The default platform to assign
   *        the event to
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  protected[registry] def toUnstructEventParams(tracker: String, qsParams: RawEventParameters, schema: String,
    eventJson: JValue, platform: String): RawEventParameters = {

    val json = compact {
      ("schema" -> UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
        ("data"   -> eventJson)
      ))
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> qsParams.getOrElse("p", platform), // Required field
      "ue_pr" -> json) ++
    qsParams.filterKeys(Set("nuid", "aid", "cv", "url"))
  }

  /**
   * USAGE: Multiple event payloads
   *
   * Processes a list of Validated RawEvents 
   * into a ValidatedRawEvents object. If there
   * were any Failures in the list we will only
   * return these.
   *
   * @param rawEventsList The list of RawEvents that needs
   *        to be processed
   * @return the ValidatedRawEvents which will be comprised
   *         of either Successful RawEvents or Failures
   */
  protected[registry] def rawEventsListProcessor(rawEventsList: List[Validated[RawEvent]]): ValidatedRawEvents = {

    val successes: List[RawEvent] = 
      for {
        Success(s) <- rawEventsList 
      } yield s

    val failures: List[String] = 
      for {
        Failure(NonEmptyList(f)) <- rawEventsList 
      } yield f

    (successes, failures) match {
      case (s :: ss,     Nil) =>  NonEmptyList(s, ss: _*).success // No Failures collected.
      case (_,       f :: fs) =>  NonEmptyList(f, fs: _*).fail    // Some or all are Failures, return these.
      case (Nil,         Nil) => "List of events is empty (should never happen, not catching empty list properly)".failNel
    }
  }

  /**
   * USAGE: Single event payloads
   *
   * Gets the correct Schema URI for the event 
   * passed from the vendor payload
   *
   * @param eventOpt An Option[String] which will contain a 
   *        String or None
   * @param vendor The vendor we are doing a schema
   *        lookup for; i.e. MailChimp or PagerDuty
   * @param eventSchemaMap A map of event types linked
   *        to their relevant schema URI's
   * @return the schema for the event or a Failure-boxed String
   *         if we cannot recognize the event type
   */
  protected[registry] def lookupSchema(eventOpt: Option[String], vendor: String, eventSchemaMap: Map[String,String]): Validated[String] =
    eventOpt match {
      case None            => s"$vendor event failed: type parameter not provided - cannot determine event type".failNel
      case Some(eventType) => {
        eventType match {
          case et if eventSchemaMap.contains(et) => {
            eventSchemaMap.get(et) match {
              case None         => s"$vendor event failed: type parameter [$et] has no schema associated with it - check event-schema map".failNel
              case Some(schema) => schema.success
            }
          }
          case "" => s"$vendor event failed: type parameter is empty - cannot determine event type".failNel
          case et => s"$vendor event failed: type parameter [$et] not recognized".failNel
        }
      }
    }

  /**
   * USAGE: Multiple event payloads
   *
   * Gets the correct Schema URI for the event 
   * passed from the vendor payload
   *
   * @param eventOpt An Option[String] which will contain a 
   *        String or None
   * @param vendor The vendor we are doing a schema
   *        lookup for; i.e. MailChimp or PagerDuty
   * @param index The index of the event we are trying to
   *        get a schema URI for
   * @param eventSchemaMap A map of event types linked
   *        to their relevant schema URI's
   * @return the schema for the event or a Failure-boxed String
   *         if we cannot recognize the event type
   */
  protected[registry] def lookupSchema(eventOpt: Option[String], vendor: String, index: Int, eventSchemaMap: Map[String,String]): Validated[String] =
    eventOpt match {
      case None            => s"$vendor event at index [$index] failed: type parameter not provided - cannot determine event type".failNel
      case Some(eventType) => {
        eventType match {
          case et if eventSchemaMap.contains(et) => {
            eventSchemaMap.get(et) match {
              case None         => s"$vendor event at index [$index] failed: type parameter [$et] has no schema associated with it - check event-schema map".failNel
              case Some(schema) => schema.success
            }
          }
          case "" => s"$vendor event at index [$index] failed: type parameter is empty - cannot determine event type".failNel
          case et => s"$vendor event at index [$index] failed: type parameter [$et] not recognized".failNel
        }
      }
    }
}
