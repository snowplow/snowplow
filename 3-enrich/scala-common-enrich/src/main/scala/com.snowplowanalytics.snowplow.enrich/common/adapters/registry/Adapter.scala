/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.data.Validated._
import cats.syntax.either._
import cats.syntax.eq._
import cats.syntax.option._
import cats.syntax.validated._

import cats.effect.Clock

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import io.circe._
import io.circe.syntax._

import org.apache.http.NameValuePair

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils => JU}

trait Adapter {

  // Signature for a Formatter function
  type FormatterFunc = (RawEventParameters) => Json

  // The encoding type to be used
  val EventEncType = "UTF-8"

  private val AcceptedQueryParameters = Set("nuid", "aid", "cv", "eid", "ttm", "url")

  // Datetime format we need to convert timestamps to
  val JsonSchemaDateTimeFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  private def toStringField(seconds: Long): String = {
    val dt: DateTime = new DateTime(seconds * 1000)
    JsonSchemaDateTimeFormat.print(dt)
  }

  private val longToDateString: Json => Option[Json] = (json: Json) =>
    json
      .as[Long]
      .toOption
      .map(v => Json.fromString(toStringField(v)))

  private val stringToDateString: Json => Json = (json: Json) =>
    json
      .mapString { v =>
        Either
          .catchNonFatal(toStringField(v.toLong))
          .getOrElse(v)
      }

  /**
   * Returns an updated event JSON where all of the timestamp fields (tsFieldKey:_) have been
   * changed to a valid JsonSchema date-time format and the "event":_type field has been removed
   * @param json The event JSON which we need to update values for
   * @param eventOpt The event type as an Option[String] which we are now going to remove from
   * the event JSON
   * @param tsFieldKeys the key name of the timestamp field which will be transformed
   * @return the updated JSON with valid date-time values in the tsFieldKey fields
   */
  private[registry] def cleanupJsonEventValues(
    json: Json,
    eventOpt: Option[(String, String)],
    tsFieldKeys: List[String]
  ): Json =
    json
      .mapObject { obj =>
        val updatedObj = obj.toMap.map {
          case (k, v) if tsFieldKeys.contains(k) && v.isString => (k, stringToDateString(v))
          case (k, v) if tsFieldKeys.contains(k) => (k, longToDateString(v).getOrElse(v))
          case (k, v) if v.isObject => (k, cleanupJsonEventValues(v, eventOpt, tsFieldKeys))
          case (k, v) if v.isArray => (k, cleanupJsonEventValues(v, eventOpt, tsFieldKeys))
          case (k, v) => (k, v)
        }
        JsonObject(
          eventOpt
            .map {
              case (k1, v1) =>
                updatedObj.filter { case (k2, v2) => !(k1 == k2 && Json.fromString(v1) === v2) }
            }
            .getOrElse(updatedObj)
            .toList: _*
        )
      }
      .mapArray(_.map(cleanupJsonEventValues(_, eventOpt, tsFieldKeys)))

  /**
   * Converts a CollectorPayload instance into raw events.
   * @param payload The CollectorPaylod containing one or more raw events as collected by a
   * Snowplow collector
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](payload: CollectorPayload, client: Client[F, Json]): F[
    ValidatedNel[FailureDetails.AdapterFailureOrTrackerProtocolViolation, NonEmptyList[RawEvent]]
  ]

  /**
   * Converts a NonEmptyList of name:value pairs into a Map.
   * @param parameters A NonEmptyList of name:value pairs
   * @return the name:value pairs in Map form
   */
  protected[registry] def toMap(parameters: List[NameValuePair]): Map[String, String] =
    parameters.map(p => p.getName -> p.getValue).toMap

  /**
   * Convenience function to build a simple formatter of RawEventParameters.
   * @param bools A List of keys whose values should be processed as boolean-like Strings
   * @param ints A List of keys whose values should be processed as integer-like Strings
   * @param dateTimes If Some, a NEL of keys whose values should be treated as date-time-like Strings,
   * which will require processing from the specified format
   * @return a formatter function which converts RawEventParameters into a cleaned JObject
   */
  protected[registry] def buildFormatter(
    bools: List[String] = Nil,
    ints: List[String] = Nil,
    dateTimes: JU.DateTimeFields = None
  ): FormatterFunc = { parameters: RawEventParameters =>
    val jsons = parameters.toList
      .map(p => JU.toJson(p._1, p._2, bools, ints, dateTimes))
    Json.obj(jsons: _*)
  }

  /**
   * Fabricates a Snowplow unstructured event from the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event must contain e, p and tv parameters, so we
   * make sure to set those.
   * @param tracker The name and version of this tracker
   * @param parameters The raw-event parameters we will nest into the unstructured event
   * @param schema The schema key which defines this unstructured event as a String
   * @param formatter A function to take the raw event parameters and turn them into a correctly
   * formatted JObject that should pass JSON Schema validation
   * @param platform The default platform to assign the event to
   * @return the raw-event parameters for a valid Snowplow unstructured event
   */
  protected[registry] def toUnstructEventParams(
    tracker: String,
    parameters: RawEventParameters,
    schema: SchemaKey,
    formatter: FormatterFunc,
    platform: String
  ): RawEventParameters = {
    val params = formatter(parameters - ("nuid", "aid", "cv", "p"))
    val json = toUnstructEvent(SelfDescribingData(schema, params)).noSpaces
    Map(
      "tv" -> tracker,
      "e" -> "ue",
      "p" -> parameters.getOrElse("p", platform), // Required field
      "ue_pr" -> json
    ) ++
      parameters.filterKeys(AcceptedQueryParameters)
  }

  /**
   * Creates a Snowplow unstructured event by nesting the provided JValue in a self-describing
   * envelope for the unstructured event.
   * @param eventJson The event which we will nest into the unstructured event
   * @return the self-describing unstructured event
   */
  protected[registry] def toUnstructEvent(eventJson: SelfDescribingData[Json]): Json =
    SelfDescribingData(Adapter.UnstructEvent, eventJson.asJson).asJson

  /**
   * Creates a Snowplow custom contexts entity by nesting the provided JValue in a self-describing
   * envelope for the custom contexts.
   * @param contextJson The context which will be nested into the custom contexts envelope
   * @return the self-describing custom contexts
   */
  protected[registry] def toContext(contextJson: SelfDescribingData[Json]): Json =
    toContexts(List(contextJson))

  /**
   * Creates a Snowplow custom contexts entity by nesting the provided JValues in a
   * self-describing envelope for the custom contexts.
   * @param contextJsons The contexts which will be nested into the custom contexts envelope
   * @return the self-describing custom contexts
   */
  protected[registry] def toContexts(contextJsons: List[SelfDescribingData[Json]]): Json =
    SelfDescribingData(Adapter.Contexts, Json.arr(contextJsons.map(_.asJson): _*)).asJson

  /**
   * Fabricates a Snowplow unstructured event from the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event must contain e, p and tv parameters, so we
   * make sure to set those.
   * @param tracker The name and version of this tracker
   * @param qsParams The query-string parameters we will nest into the unstructured event
   * @param schema The schema key which defines this unstructured event as a String
   * @param eventJson The event which we will nest into the unstructured event
   * @param platform The default platform to assign the event to
   * @return the raw-event parameters for a valid Snowplow unstructured event
   */
  protected[registry] def toUnstructEventParams(
    tracker: String,
    qsParams: RawEventParameters,
    schema: SchemaKey,
    eventJson: JsonObject,
    platform: String
  ): RawEventParameters = {
    val json = toUnstructEvent(SelfDescribingData(schema, eventJson.asJson)).noSpaces
    Map(
      "tv" -> tracker,
      "e" -> "ue",
      "p" -> qsParams.getOrElse("p", platform), // Required field
      "ue_pr" -> json
    ) ++
      qsParams.filterKeys(AcceptedQueryParameters)
  }

  /**
   * Fabricates a Snowplow unstructured event from the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event must contain e, p and tv parameters, so we
   * make sure to set those.
   * @param tracker The name and version of this tracker
   * @param qsParams The query-string parameters we will nest into the unstructured event
   * @param schema The schema key which defines this unstructured event as a String
   * @param eventJson The event which we will nest into the unstructured event
   * @param platform The default platform to assign the event to
   * @return the raw-event parameters for a valid Snowplow unstructured event
   */
  protected[registry] def toUnstructEventParams(
    tracker: String,
    qsParams: RawEventParameters,
    schema: SchemaKey,
    eventJson: Json,
    platform: String
  ): RawEventParameters = {
    val json = toUnstructEvent(SelfDescribingData(schema, eventJson)).noSpaces

    Map(
      "tv" -> tracker,
      "e" -> "ue",
      "p" -> qsParams.getOrElse("p", platform), // Required field
      "ue_pr" -> json
    ) ++
      qsParams.filterKeys(AcceptedQueryParameters)
  }

  /**
   * USAGE: Multiple event payloads
   * Processes a list of Validated RawEvents
   * into a ValidatedRawEvents object. If there
   * were any Failures in the list we will only
   * return these.
   * @param rawEventsList The list of RawEvents that needs to be processed
   * @return the ValidatedRawEvents which will be comprised of either Successful RawEvents
   * or Failures
   */
  protected[registry] def rawEventsListProcessor(
    rawEventsList: List[ValidatedNel[FailureDetails.AdapterFailure, RawEvent]]
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] = {
    val successes: List[RawEvent] =
      for {
        Valid(s) <- rawEventsList
      } yield s

    val failures: List[FailureDetails.AdapterFailure] =
      (for {
        Invalid(NonEmptyList(h, t)) <- rawEventsList
      } yield h :: t).flatten

    (successes, failures) match {
      // No Failures collected.
      case (s :: ss, Nil) => NonEmptyList.of(s, ss: _*).valid
      // Some or all are Failures, return these.
      case (_, f :: fs) => NonEmptyList.of(f, fs: _*).invalid
      case (Nil, Nil) =>
        FailureDetails.AdapterFailure
          .InputData("", none, "empty list of events")
          .invalidNel
    }
  }

  /**
   * USAGE: Single event payloads
   * Gets the correct Schema URI for the event passed from the vendor payload
   * @param eventOpt An Option[String] which will contain a String or None
   * @param eventSchemaMap A map of event types linked to their relevant schema URI's
   * @return the schema for the event or a Failure-boxed String if we cannot recognize the
   * event type
   */
  protected[registry] def lookupSchema(
    eventOpt: Option[String],
    eventSchemaMap: Map[String, SchemaKey]
  ): Either[FailureDetails.AdapterFailure, SchemaKey] =
    eventOpt match {
      case None =>
        val msg = "cannot determine event type: type parameter not provided"
        FailureDetails.AdapterFailure.SchemaMapping(None, eventSchemaMap, msg).asLeft
      case Some(eventType) =>
        eventType match {
          case et if eventSchemaMap.contains(et) =>
            eventSchemaMap.get(et) match {
              case None =>
                val msg = "no schema associated with the provided type parameter"
                FailureDetails.AdapterFailure
                  .SchemaMapping(eventType.some, eventSchemaMap, msg)
                  .asLeft
              case Some(schema) => schema.asRight
            }
          case "" =>
            val msg = "cannot determine event type: type parameter empty"
            FailureDetails.AdapterFailure
              .SchemaMapping(eventType.some, eventSchemaMap, msg)
              .asLeft
          case _ =>
            val msg = "no schema associated with the provided type parameter"
            FailureDetails.AdapterFailure
              .SchemaMapping(eventType.some, eventSchemaMap, msg)
              .asLeft
        }
    }

  /**
   * USAGE: Multiple event payloads
   * Gets the correct Schema URI for the event passed from the vendor payload
   * @param eventOpt An Option[String] which will contain a String or None
   * @param index The index of the event we are trying to get a schema URI for
   * @param eventSchemaMap A map of event types linked to their relevant schema URI's
   * @return the schema for the event or a Failure-boxed String if we cannot recognize the
   * event type
   */
  protected[registry] def lookupSchema(
    eventOpt: Option[String],
    index: Int,
    eventSchemaMap: Map[String, SchemaKey]
  ): Either[FailureDetails.AdapterFailure, SchemaKey] =
    eventOpt match {
      case None =>
        val msg = s"cannot determine event type: type parameter not provided at index $index"
        FailureDetails.AdapterFailure.SchemaMapping(None, eventSchemaMap, msg).asLeft
      case Some(eventType) =>
        eventType match {
          case et if eventSchemaMap.contains(et) =>
            eventSchemaMap.get(et) match {
              case None =>
                val msg = s"no schema associated with the provided type parameter at index $index"
                FailureDetails.AdapterFailure
                  .SchemaMapping(eventType.some, eventSchemaMap, msg)
                  .asLeft
              case Some(schema) => schema.asRight
            }
          case "" =>
            val msg = s"cannot determine event type: type parameter empty at index $index"
            FailureDetails.AdapterFailure
              .SchemaMapping(eventType.some, eventSchemaMap, msg)
              .asLeft
          case _ =>
            val msg = s"no schema associated with the provided type parameter at index $index"
            FailureDetails.AdapterFailure
              .SchemaMapping(eventType.some, eventSchemaMap, msg)
              .asLeft
        }
    }

  private[registry] val snakeCaseOrDashTokenCapturingRegex = "[_-](\\w)".r

  /**
   * Converts dash or unersocre separted strings to camelCase.
   * @param snakeOrDash string like "X-Mailgun-Sid" or "x_mailgun_sid"
   * @return string like "xMailgunSid"
   */
  private[registry] def camelCase(snakeOrDash: String) =
    snakeCaseOrDashTokenCapturingRegex.replaceAllIn(
      Character.toLowerCase(snakeOrDash.charAt(0)) + snakeOrDash.substring(1),
      m => m.group(1).capitalize
    )

  /**
   * Converts input field case to camel case recursively
   * @param json parsed event fields as a JValue
   * @return The mutated event.
   */
  private[registry] def camelize(json: Json): Json =
    json.asObject match {
      case Some(obj) =>
        Json.obj(obj.toList.map { case (k, v) => (camelCase(k), camelize(v)) }: _*)
      case None =>
        json.asArray match {
          case Some(arr) => Json.arr(arr.map(camelize): _*)
          case None => json
        }
    }
}

object Adapter {

  /** The Iglu schema URI for a Snowplow unstructured event */
  val UnstructEvent = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "unstruct_event",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  /** The Iglu schema URI for a Snowplow custom contexts */
  val Contexts = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "contexts",
    "jsonschema",
    SchemaVer.Full(1, 0, 1)
  )
}
