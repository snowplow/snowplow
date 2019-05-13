/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow

import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.data.Validated._
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.badrows._
import io.circe.Json

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils => JU}

/**
 * Version 2 of the Tracker Protocol supports GET and POST. Note that with POST, data can still be
 * passed on the querystring.
 */
object Tp2Adapter extends Adapter {
  // Expected content types for a request body
  private object ContentTypes {
    val list =
      List("application/json", "application/json; charset=utf-8", "application/json; charset=UTF-8")
    val str = list.mkString(", ")
  }

  // Request body expected to validate against this JSON Schema
  private val PayloadDataSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "payload_data", "jsonschema", 1, 0)

  /**
   * Converts a CollectorPayload instance into N raw events.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[
    ValidatedNel[FailureDetails.AdapterFailureOrTrackerProtocolViolation, NonEmptyList[RawEvent]]
  ] = {
    val qsParams = toMap(payload.querystring)

    // Verify: body + content type set; content type matches expected; body contains expected JSON Schema; body passes schema validation
    val validatedParamsNel
      : F[ValidatedNel[FailureDetails.TrackerProtocolViolation, NonEmptyList[RawEventParameters]]] =
      (payload.body, payload.contentType) match {
        case (None, _) if qsParams.isEmpty =>
          val msg1 = "empty body: not a valid tracker protocol event"
          val failure1 = FailureDetails.TrackerProtocolViolation.InputData(
            "body",
            none,
            msg1
          )
          val msg2 = "empty querystring: not a valid tracker protocol event"
          val failure2 = FailureDetails.TrackerProtocolViolation.InputData(
            "querystring",
            none,
            msg2
          )
          Monad[F].pure(NonEmptyList.of(failure1, failure2).invalid)
        case (_, Some(ct)) if !ContentTypes.list.contains(ct) =>
          val msg = s"expected one of ${ContentTypes.str}"
          val failure = FailureDetails.TrackerProtocolViolation.InputData(
            "contentType",
            ct.some,
            msg
          )
          Monad[F].pure(failure.invalidNel)
        case (Some(_), None) =>
          val msg = s"expected one of ${ContentTypes.str}"
          val failure = FailureDetails.TrackerProtocolViolation.InputData(
            "contentType",
            none,
            msg
          )
          Monad[F].pure(failure.invalidNel)
        case (None, Some(_)) =>
          val msg = "empty body: not a valid track protocol event"
          val failure = FailureDetails.TrackerProtocolViolation.InputData(
            "body",
            none,
            msg
          )
          Monad[F].pure(failure.invalidNel)
        case (None, None) => Monad[F].pure(NonEmptyList.one(qsParams).valid)
        case (Some(bdy), Some(_)) => // Build our NEL of parameters
          (for {
            json <- extractAndValidateJson(PayloadDataSchema, bdy, "body", client)
            nel <- EitherT.fromEither[F](toParametersNel(json, qsParams))
          } yield nel).toValidated
      }

    validatedParamsNel.map { f =>
      f.map { params =>
        params.map { p =>
          RawEvent(payload.api, p, payload.contentType, payload.source, payload.context)
        }
      }
    }
  }

  /**
   * Converts a JSON Node into a Validated NEL of parameters for a RawEvent. The parameters
   * take the form Map[String, String].
   * Takes a second set of parameters to merge with the generated parameters (the second set takes
   * precedence in case of a clash).
   * @param instance The JSON to convert
   * @param mergeWith A second set of parameters to merge (and possibly overwrite) parameters
   * from the instance
   * @return a NEL of Map[String, String] parameters on Succeess, a NEL of Strings on Failure
   */
  private def toParametersNel(instance: Json, mergeWith: RawEventParameters): Either[NonEmptyList[
    FailureDetails.TrackerProtocolViolation
  ], NonEmptyList[RawEventParameters]] = {
    val events: Option[
      Vector[Vector[Validated[FailureDetails.TrackerProtocolViolation, (String, String)]]]
    ] = for {
      topLevel <- instance.asArray
      fields <- topLevel.map(_.asObject).sequence
      res = fields.map(_.toVector.map(toParameter))
    } yield res

    events match {
      case Some(events) =>
        val failures = events.flatten.collect { case Invalid(f) => f }
        // We don't bother doing this conditionally because we
        // don't expect any failures, so any performance gain
        // from conditionality would be miniscule
        val successes =
          events.map(_.collect { case Valid(p) => p }.toMap ++ mergeWith)

        (successes.toList, failures.toList) match {
          case (s :: ss, Nil) => NonEmptyList.of(s, ss: _*).asRight // No Failures collected
          case (_ :: _, f :: fs) =>
            // Some Failures, return those. Should never happen, unless JSON Schema changed
            NonEmptyList.of(f, fs: _*).asLeft
          case (Nil, _) =>
            NonEmptyList
              .one(
                FailureDetails.TrackerProtocolViolation.InputData(
                  "body",
                  instance.noSpaces.some,
                  "empty list of events"
                )
              )
              .asLeft
        }
      case None =>
        NonEmptyList
          .one(
            FailureDetails.TrackerProtocolViolation.InputData(
              "body",
              instance.noSpaces.some,
              "json is not an array"
            )
          )
          .asLeft
    }
  }

  /**
   * Converts a (String, Json) into a (String -> String) parameter.
   * @param entry The (String, Json) to convert
   * @return a Validation boxing either our parameter on Success, or an error String on Failure.
   */
  private def toParameter(
    entry: (String, Json)
  ): Validated[FailureDetails.TrackerProtocolViolation, (String, String)] = {
    val (key, value) = entry

    value.asString match {
      case Some(txt) => (key, txt).valid
      case None if value.isNull =>
        FailureDetails.TrackerProtocolViolation
          .InputData(key, none, "value cannot be null")
          .invalid
      case _ =>
        FailureDetails.TrackerProtocolViolation
          .InputData(key, value.noSpaces.some, "value is not a string")
          .invalid
    }
  }

  /**
   * Extract the JSON from a String, and validate it against the supplied JSON Schema.
   * @param field The name of the field containing the JSON instance
   * @param schemaCriterion The schema that we expected this self-describing JSON to conform to
   * @param instance A JSON instance as String
   * @param client Our Iglu client, for schema lookups
   * @return an Option-boxed Validation containing either a Nel of JsonNodes error message on
   * Failure, or a singular JsonNode on success
   */
  private def extractAndValidateJson[F[_]: Monad: RegistryLookup: Clock](
    schemaCriterion: SchemaCriterion,
    instance: String,
    field: String,
    client: Client[F, Json]
  ): EitherT[F, NonEmptyList[FailureDetails.TrackerProtocolViolation], Json] =
    (for {
      j <- EitherT.fromEither[F](
        JU.extractJson(instance)
          .leftMap(
            e =>
              NonEmptyList.one(
                FailureDetails.TrackerProtocolViolation
                  .NotJson(field, instance.some, e)
              )
          )
      )
      sd <- EitherT.fromEither[F](
        SelfDescribingData
          .parse(j)
          .leftMap(
            e =>
              NonEmptyList.one(
                FailureDetails.TrackerProtocolViolation
                  .NotSD(instance, e.code)
              )
          )
      )
      _ <- client
        .check(sd)
        .leftMap(
          e =>
            NonEmptyList.one(
              FailureDetails.TrackerProtocolViolation
                .IgluError(sd.schema, e)
            )
        )
        .subflatMap { _ =>
          schemaCriterion.matches(sd.schema) match {
            case true => ().asRight
            case false =>
              NonEmptyList
                .one(
                  FailureDetails.TrackerProtocolViolation
                    .SchemaCrit(sd.schema, schemaCriterion)
                )
                .asLeft
          }
        }
    } yield sd.data).leftWiden[NonEmptyList[FailureDetails.TrackerProtocolViolation]]
}
