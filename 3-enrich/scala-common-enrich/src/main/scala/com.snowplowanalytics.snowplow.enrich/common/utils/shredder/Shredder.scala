/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package utils
package shredder

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.SchemaViolation._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

import outputs.EnrichedEvent

/**
 * The shredder takes the two fields containing JSONs (contexts and unstructured event properties)
 * and "shreds" their contents into a List of Jsons ready for loading into dedicated tables in
 * the database.
 */
object Shredder {

  // All shredded JSONs have the events type (aka table) as their ultimate parent
  private val TypeHierarchyRoot = "events"

  // Self-describing schema for a ue_properties
  private val UePropertiesSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", 1, 0)

  // Self-describing schema for a contexts
  private val ContextsSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "contexts", "jsonschema", 1, 0)

  /**
   * Shred the EnrichedEvent's two fields which contain JSONs: contexts and unstructured event
   * properties. By shredding we mean:
   * 1. Verify the two fields contain valid JSONs
   * 2. Validate they conform to JSON Schema
   * 3. For the contexts, break the singular Json into a List of individual context Json
   * 4. Collect the unstructured event and contexts into a singular List
   * @param event The Snowplow enriched event to shred JSONs from
   * @param client The Iglu client used for schema lookup
   * @return a Validation containing on Success a List (possible empty) of Json and on Failure
   * a NonEmptyList of Json containing error messages
   */
  def shred[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] = {
    // Define what we know so far of the type hierarchy.
    val partialHierarchy = makePartialHierarchy(event.event_id, event.collector_tstamp)

    // Joining all validated JSONs into a single validated List[Json], collecting Failures too
    for {
      ue <- extractAndValidateUnstructEvent(event, client)
      c <- extractAndValidateCustomContexts(event, client)
      dc <- extractAndValidateDerivedContexts(event, client)
    } yield (ue |+| c |+| dc).map(_.map(sd => attachMetadata(sd, partialHierarchy)))
  }

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
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    flatten(extractUnstructEvent(event, client))
      .flatMap(extracted => validate(extracted, client))

  /**
   * Extract list of custom contexts out of string and validate each against its schema
   * @param event The Snowplow enriched event to extract custom context JSONs from
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  def extractAndValidateCustomContexts[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    extractAndValidateContexts(event.contexts, "contexts", client)

  /**
   * Extract list of derived contexts out of string and validate each against its schema
   * @param event The Snowplow enriched event to extract custom context JSONs from
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  def extractAndValidateDerivedContexts[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    extractAndValidateContexts(event.derived_contexts, "derived_contexts", client)

  /**
   * Extract list of contexts out of string and validate each against its schema
   * @param json string supposed to contain Snowplow Contexts object
   * @param field field where object is came from (used only for error log)
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  private[shredder] def extractAndValidateContexts[F[_]: Monad: RegistryLookup: Clock](
    json: String,
    field: String,
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    flatten(extractContexts(json, field, client))
      .flatMap(extracted => validate(extracted, client))

  /**
   * Extract unstruct event as Json. Extraction involves validation against schema.
   * Event itself extracted as List (empty or with single element)
   * @param event The Snowplow enriched event to shred JSONs from
   * @param client The Iglu client used for schema lookup
   * @return a Validation containing on Success a List (possible empty) of Json and on Failure
   * a NonEmptyList of Json containing error messages
   */
  def extractUnstructEvent[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): Option[F[ValidatedNel[EnrichmentStageIssue, List[Json]]]] =
    Option(event.unstruct_event).map { ue =>
      extractAndValidateJson(
        "ue_properties",
        UePropertiesSchema,
        ue,
        client
      ).map(_.map(_ :: Nil))
    }

  /**
   * Extract list of contexts out of string. Extraction involves validation against schema
   * @param json string with contexts object
   * @param field field where object is came from (used only for error log)
   * @param client The Iglu client used for schema lookup
   * @return an Optional Validation containing on Success a List (possible empty) of Json
   * and on Failure a NonEmptyList of Json containing error messages
   */
  private[shredder] def extractContexts[F[_]: Monad: RegistryLookup: Clock](
    json: String,
    field: String,
    client: Client[F, Json]
  ): Option[F[ValidatedNel[EnrichmentStageIssue, List[Json]]]] =
    Option(json).map { js =>
      extractAndValidateJson(field, ContextsSchema, js, client)
        .map(_.map { json =>
          json.asArray match {
            case Some(js) => js.toList
            case None => json :: Nil
          }
        })
    }

  /**
   * Fetch Iglu Schema for each [[Json]] in [[ValidatedNelMessage]] and validate this node
   * against it
   * @param validatedJsons list of valid JSONs supposed to be Self-describing
   * @param client The Iglu client used for schema lookup
   * @return validated list of pairs consist of schema and node
   */
  private[shredder] def validate[F[_]: Monad: RegistryLookup: Clock](
    validatedJsons: ValidatedNel[EnrichmentStageIssue, List[Json]],
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, List[SelfDescribingData[Json]]]] =
    (for {
      jsons <- EitherT.fromEither[F](validatedJsons.toEither)
      validated <- jsons.map { json =>
        for {
          sd <- EitherT.fromEither[F](
            SelfDescribingData
              .parse(json)
              .leftMap { e =>
                NonEmptyList.one(NotSDSchemaViolation(json.noSpaces, e.code): EnrichmentStageIssue)
              }
          )
          _ <- client
            .check(sd)
            .leftMap(
              e => NonEmptyList.one(IgluErrorSchemaViolation(sd.schema, e): EnrichmentStageIssue)
            )
        } yield sd
      }.sequence
    } yield validated).value.map(_.toValidated)

  /**
   * Flatten Option[List] to List
   * @param o Option with List
   * @return empty list in case of None, or non-empty in case of some
   */
  private[shredder] def flatten[F[_]: Monad](
    o: Option[F[ValidatedNel[EnrichmentStageIssue, List[Json]]]]
  ): F[ValidatedNel[EnrichmentStageIssue, List[Json]]] = o match {
    case Some(vjl) => vjl
    case None => Monad[F].pure(List.empty[Json].validNel)
  }

  /**
   * Convenience to make a partial TypeHierarchy. Partial because we don't have the complete
   * refTree yet.
   * @param rootId The ID of the root element
   * @param rootTstamp The timestamp of the root element
   * @return the partially complete TypeHierarchy
   */
  private[shredder] def makePartialHierarchy(rootId: String, rootTstamp: String): TypeHierarchy =
    TypeHierarchy(
      rootId = rootId,
      rootTstamp = rootTstamp,
      refRoot = TypeHierarchyRoot,
      refTree = List(TypeHierarchyRoot), // This is a partial tree. Need to complete later
      refParent = TypeHierarchyRoot // Hardcode as nested shredding not supported yet
    )

  /**
   * Adds shred-related metadata to the JSON. There are two envelopes of metadata to attach:
   * 1. schema - we replace the existing schema URI string with a full schema key object containing
   * name, vendor, format and version as separate string properties
   * 2. hierarchy - we add a new object expressing the type hierarchy for this shredded JSON
   * @param instanceSchemaPair Tuple2 containing:
   *        1. The SchemaKey identifying the schema for this JSON
   *        2. The Json for this JSON
   * @param partialHierarchy The type hierarchy to attach. Partial because the refTree is still
   * incomplete
   * @return the Tuple2, with the JSON updated to contain the full schema key, plus the
   * now-finalized hierarchy
   */
  private def attachMetadata(
    instanceSchemaPair: SelfDescribingData[Json],
    partialHierarchy: TypeHierarchy
  ): SelfDescribingData[Json] = {
    val schema = instanceSchemaPair.schema
    val hierarchy = partialHierarchy.complete(List(schema.name))

    val updated = instanceSchemaPair.data.mapObject { j =>
      j.add("schema", Json.fromString(schema.toSchemaUri))
        .add("hierarchy", hierarchy.asJson)
    }

    SelfDescribingData[Json](schema, updated)
  }

  /**
   * Extract the JSON from a String, and validate it against the supplied JSON Schema.
   * @param field The name of the field containing the JSON instance
   * @param schemaCriterion The criterion we expected this self-describing JSON to conform to
   * @param instance An Option-boxed JSON instance
   * @param client The Iglu client used for schema lookup
   * @return an Option-boxed Validation containing either a Nel of Json error message on
   * Failure, or a singular Json on success
   */
  private def extractAndValidateJson[F[_]: Monad: RegistryLookup: Clock](
    field: String,
    schemaCriterion: SchemaCriterion,
    instance: String,
    client: Client[F, Json]
  ): F[ValidatedNel[EnrichmentStageIssue, Json]] =
    (for {
      j <- EitherT.fromEither[F](
        JsonUtils
          .extractJson(instance)
          .leftMap(e => NonEmptyList.one(NotJsonSchemaViolation(field, instance.some, e)))
      )
      sd <- EitherT.fromEither[F](
        SelfDescribingData
          .parse(j)
          .leftMap(e => NonEmptyList.one(NotSDSchemaViolation(instance, e.code)))
      )
      _ <- client
        .check(sd)
        .leftMap(e => NonEmptyList.one(IgluErrorSchemaViolation(sd.schema, e)))
        .subflatMap { _ =>
          schemaCriterion.matches(sd.schema) match {
            case true => ().asRight
            case false =>
              NonEmptyList
                .one(SchemaCritSchemaViolation(sd.schema, schemaCriterion))
                .asLeft
          }
        }
    } yield sd.data).leftWiden[NonEmptyList[EnrichmentStageIssue]].toValidated
}
