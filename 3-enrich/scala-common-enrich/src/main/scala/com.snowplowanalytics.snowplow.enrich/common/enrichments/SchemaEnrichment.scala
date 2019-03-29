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
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
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
  ): F[Either[String, SchemaKey]] =
    event.event match {
      case "page_view" => Monad[F].pure(Schemas.pageViewSchema.asRight)
      case "page_ping" => Monad[F].pure(Schemas.pagePingSchema.asRight)
      case "struct" => Monad[F].pure(Schemas.structSchema.asRight)
      case "transaction" => Monad[F].pure(Schemas.transactionSchema.asRight)
      case "transaction_item" => Monad[F].pure(Schemas.transactionItemSchema.asRight)
      case "unstruct" => extractUnstructSchema(event, client)
      case eventType => Monad[F].pure(s"Unrecognized event [$eventType]".asLeft)
    }

  private def extractUnstructSchema[F[_]: Monad: RegistryLookup: Clock](
    event: EnrichedEvent,
    client: Client[F, Json]
  ): F[Either[String, SchemaKey]] =
    Shredder.extractUnstructEvent(event, client) match {
      case Some(f) => f.map {
        case Validated.Valid(List(json)) => parseSchemaKey(json.asObject.flatMap(_.apply("schema")))
        case _ => "Unstructured event couldn't be extracted".asLeft
      }
      case _ => Monad[F].pure("Unstructured event couldn't be extracted".asLeft)
    }

  private def parseSchemaKey(node: Option[Json]): Either[String, SchemaKey] =
    node.flatMap(_.asString) match {
      case Some(str) => SchemaKey.fromUri(str).leftMap(_.code)
      // It's validated by the Shredder, so it should never happen
      case _ => "Unrecognized unstructured event structure".asLeft
    }
}
