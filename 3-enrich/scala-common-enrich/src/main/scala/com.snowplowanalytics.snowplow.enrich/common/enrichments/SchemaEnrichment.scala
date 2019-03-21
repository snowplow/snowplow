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

import cats.data.Validated
import cats.syntax.either._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.snowplowanalytics.iglu.client.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver

import outputs.EnrichedEvent
import utils.shredder.Shredder

object SchemaEnrichment {

  private object Schemas {
    val pageViewSchema =
      SchemaKey("com.snowplowanalytics.snowplow", "page_view", "jsonschema", "1-0-0")
    val pagePingSchema =
      SchemaKey("com.snowplowanalytics.snowplow", "page_ping", "jsonschema", "1-0-0")
    val transactionSchema =
      SchemaKey("com.snowplowanalytics.snowplow", "transaction", "jsonschema", "1-0-0")
    val transactionItemSchema =
      SchemaKey("com.snowplowanalytics.snowplow", "transaction_item", "jsonschema", "1-0-0")
    val structSchema = SchemaKey("com.google.analytics", "event", "jsonschema", "1-0-0")
  }

  def extractSchema(event: EnrichedEvent)(
    implicit resolver: Resolver
  ): Either[String, SchemaKey] =
    event.event match {
      case "page_view" => Schemas.pageViewSchema.asRight
      case "page_ping" => Schemas.pagePingSchema.asRight
      case "struct" => Schemas.structSchema.asRight
      case "transaction" => Schemas.transactionSchema.asRight
      case "transaction_item" => Schemas.transactionItemSchema.asRight
      case "unstruct" => extractUnstructSchema(event)
      case eventType => s"Unrecognized event [$eventType]".asLeft
    }

  private def extractUnstructSchema(event: EnrichedEvent)(
    implicit resolver: Resolver
  ): Either[String, SchemaKey] =
    Shredder.extractUnstructEvent(event) match {
      case Some(Validated.Valid(List(json))) =>
        parseSchemaKey(Option(json.get("schema")))
      case _ => "Unstructured event couldn't be extracted".asLeft
    }

  private def parseSchemaKey(node: Option[JsonNode]): Either[String, SchemaKey] = node match {
    case Some(textNode: TextNode) =>
      SchemaKey.parse(textNode.textValue()).toEither.leftMap(_.toString)
    case _ =>
      // It's validated by the Shredder, so it should never happen
      "Unrecognized unstructured event structure".asLeft
  }
}
