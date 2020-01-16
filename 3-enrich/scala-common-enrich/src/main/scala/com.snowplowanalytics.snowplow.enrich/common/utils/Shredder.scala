/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.Monad
import cats.data.EitherT
import cats.effect.Clock
import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/**
 * The shredder takes the two fields containing JSONs (contexts and unstructured event properties)
 * and "shreds" their contents into a List of Jsons ready for loading into dedicated tables in
 * the database.
 */
object Shredder {

  // Self-describing schema for a ue_properties
  private val UePropertiesSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", 1, 0)

  // Self-describing schema for a contexts
  private val ContextsSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "contexts", "jsonschema", 1, 0)

  /**
   * Extract unstruct event out of EnrichedEvent and validate against it's schema
   * @param event The Snowplow enriched event to find unstruct event in
   * @param client The Iglu client used for schema lookup
   * @return validated list (empty or single-element) of pairs consist of unstruct event schema and
   * node
   */
  def extractAndValidateUnstructEvent[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[FailureDetails.EnrichmentStageIssue, Option[SelfDescribingData[Json]]]] =
    extractUnstructEvent(event, client).flatMap {
      case Some(instance) =>
        check(instance, client).as(instance.some)
      case None =>
        EitherT.rightT[F, FailureDetails.EnrichmentStageIssue](none[SelfDescribingData[Json]])
    }.value

  /** Extract unstruct event out of EnrichedEvent and validate against unstruct_event schema (no inner schema!) */
  def extractUnstructEvent[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): EitherT[F, FailureDetails.EnrichmentStageIssue, Option[SelfDescribingData[Json]]] =
    Option(event.unstruct_event).traverse { unstructEvent =>
      parseAndValidate("ue_properties", UePropertiesSchema, unstructEvent, client)
        .subflatMap(parseSelfDescribing)
    }

  /**
   * Extract list of custom contexts out of string and validate each against its schema
   * @param event The Snowplow enriched event to extract custom context JSONs from
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  def extractAndValidateCustomContexts[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[FailureDetails.EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    Option(event.contexts) match {
      case Some(contexts) =>
        extractAndValidateContexts(contexts, "contexts", client)
      case None =>
        Monad[F].pure(List.empty[SelfDescribingData[Json]].asRight)
    }

  /**
   * Extract list of derived contexts out of string and validate each against its schema
   * @param event The Snowplow enriched event to extract custom context JSONs from
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  def extractAndValidateDerivedContexts[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[FailureDetails.EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    Option(event.derived_contexts) match {
      case Some(contexts) =>
        extractAndValidateContexts(contexts, "derived_contexts", client)
      case None =>
        Monad[F].pure(List.empty[SelfDescribingData[Json]].asRight)
    }

  /**
   * Extract list of contexts out of string and validate each against its schema
   * @param json string supposed to contain Snowplow Contexts object
   * @param field field where object is came from (used only for error log)
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  private def extractAndValidateContexts[F[_]: Monad: RegistryLookup: Clock](
    json: String,
    field: String,
    client: Client[F, Json]
  ): F[Either[FailureDetails.EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    extractContexts(json, field, client).flatMap { extracted =>
      extracted.traverse(context => validate(context, client, None))
    }.value

  /**
   * Extract list of contexts out of string. Extraction involves validation against schema
   * @param json string with contexts object
   * @param field field where object is came from (used only for error log)
   * @param client The Iglu client used for schema lookup
   * @return an Optional Validation containing on Success a List (possible empty) of Json
   * and on Failure a NonEmptyList of Json containing error messages
   */
  private def extractContexts[F[_]: Monad: RegistryLookup: Clock](
    json: String,
    field: String,
    client: Client[F, Json]
  ): EitherT[F, FailureDetails.EnrichmentStageIssue, List[Json]] =
    parseAndValidate(field, ContextsSchema, json, client)
      .map { json =>
        json.asArray match {
          case Some(js) => js.toList
          case None => json :: Nil
        }
      }

  /**
   * Parse JSON as self-describing, validate against its schema and optionally
   * check against some criterion
   * @param json valid JSON supposed to be Self-describing
   * @param client The Iglu client used for schema lookup
   * @param criterion optional criterion to filter out unwanted schemas
   * @return parsed self-describing JSON
   */
  private def validate[F[_]: Monad: RegistryLookup: Clock](
    json: Json,
    client: Client[F, Json],
    criterion: Option[SchemaCriterion]
  ): EitherT[F, FailureDetails.EnrichmentStageIssue, SelfDescribingData[Json]] =
    for {
      data <- parseSelfDescribing(json).toEitherT[F]
      _ <- criterion match {
        case Some(schemaCriterion) if schemaCriterion.matches(data.schema) =>
          EitherT.rightT[F, FailureDetails.EnrichmentStageIssue](())
        case Some(schemaCriterion) =>
          EitherT.leftT[F, Unit] {
            FailureDetails.SchemaViolation.CriterionMismatch(data.schema, schemaCriterion)
          }
        case None =>
          EitherT.rightT[F, FailureDetails.EnrichmentStageIssue](())
      }
      _ <- check(data, client)
    } yield data

  /**
   * Extract the JSON from a String, and validate it against the supplied JSON Schema.
   * @param field The name of the field containing the JSON instance
   * @param schemaCriterion The criterion we expected this self-describing JSON to conform to
   * @param instance An Option-boxed JSON instance
   * @param client The Iglu client used for schema lookup
   * @return an Option-boxed Validation containing either a Nel of Json error message on
   * Failure, or a singular Json on success
   */
  private def parseAndValidate[F[_]: Monad: RegistryLookup: Clock](
    field: String,
    schemaCriterion: SchemaCriterion,
    instance: String,
    client: Client[F, Json]
  ): EitherT[F, FailureDetails.EnrichmentStageIssue, Json] = {
    val validatedData = for {
      json <- JsonUtils
        .extractJson(instance)
        .leftMap(e => FailureDetails.SchemaViolation.NotJson(field, instance.some, e))
        .toEitherT[F]
      selfDescribing <- validate(json, client, Some(schemaCriterion))
    } yield selfDescribing.data
    validatedData.leftWiden[FailureDetails.EnrichmentStageIssue]
  }

  /** `SelfDescribing.parse` with `BadRow`-specific error */
  private def parseSelfDescribing(json: Json) =
    SelfDescribingData
      .parse(json)
      .leftMap { e =>
        FailureDetails.SchemaViolation.NotIglu(json, e): FailureDetails.EnrichmentStageIssue
      }

  /** `IgluClient.check` with `BadRow`-specific error */
  private def check[F[_]: Monad: RegistryLookup: Clock](
    json: SelfDescribingData[Json],
    client: Client[F, Json]
  ) =
    client
      .check(json)
      .leftMap { e =>
        FailureDetails.SchemaViolation
          .IgluError(json.schema, e): FailureDetails.EnrichmentStageIssue
      }
}
