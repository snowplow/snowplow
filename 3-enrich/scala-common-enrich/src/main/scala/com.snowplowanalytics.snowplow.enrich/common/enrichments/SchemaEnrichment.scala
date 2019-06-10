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
package enrichments

import cats.Monad
import cats.data.Validated
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.functor._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.EnrichmentFailureMessage._
import com.snowplowanalytics.snowplow.badrows.SchemaViolation._
import io.circe.Json

import outputs.EnrichedEvent
import utils.shredder.Shredder

object SchemaEnrichment {

  private object Schemas {
    private val Vendor = "com.snowplowanalytics.snowplow"
    private val Format = "jsonschema"
    private val SchemaVersion = SchemaVer.Full(1, 0, 0)
    val pageViewSchema = SchemaKey(Vendor, "page_view", Format, SchemaVersion)
    val pagePingSchema = SchemaKey(Vendor, "page_ping", Format, SchemaVersion)
    val transactionSchema = SchemaKey(Vendor, "transaction", Format, SchemaVersion)
    val transactionItemSchema = SchemaKey(Vendor, "transaction_item", Format, SchemaVersion)
    val structSchema = SchemaKey("com.google.analytics", "event", Format, SchemaVersion)
  }

  def extractSchema[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[EnrichmentStageIssue, SchemaKey]] =
    event.event match {
      case "page_view" => Monad[F].pure(Schemas.pageViewSchema.asRight)
      case "page_ping" => Monad[F].pure(Schemas.pagePingSchema.asRight)
      case "struct" => Monad[F].pure(Schemas.structSchema.asRight)
      case "transaction" => Monad[F].pure(Schemas.transactionSchema.asRight)
      case "transaction_item" => Monad[F].pure(Schemas.transactionItemSchema.asRight)
      case "unstruct" => extractUnstructSchema(event, client)
      case eventType =>
        val f = InputDataEnrichmentFailureMessage("event", Option(eventType), "unrecognized")
        Monad[F].pure(EnrichmentFailure(None, f).asLeft)
    }

  private def extractUnstructSchema[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[EnrichmentStageIssue, SchemaKey]] = {
    val possibleFailure = {
      val f = InputDataEnrichmentFailureMessage(
        "unstruct_event",
        Option(event.unstruct_event),
        "could not be extracted"
      )
      EnrichmentFailure(None, f).asLeft
    }
    Shredder.extractUnstructEvent(event, client) match {
      case Some(f) =>
        f.map {
          case Validated.Valid(List(json)) =>
            SelfDescribingData.parse(json) match {
              case Left(parseError) =>
                NotSDSchemaViolation(event.unstruct_event, parseError.code).asLeft
              case Right(sd) => sd.schema.asRight
            }
          case _ => possibleFailure
        }
      case _ => Monad[F].pure(possibleFailure)
    }
  }
}
