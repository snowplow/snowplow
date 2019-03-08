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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.snowplowanalytics.iglu.client.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver
import scalaz._
import Scalaz._

import outputs.EnrichedEvent
import utils.shredder.Shredder

object SchemaEnrichment {

  private object Schemas {
    val pageViewSchema = SchemaKey("com.snowplowanalytics.snowplow", "page_view", "jsonschema", "1-0-0").success
    val pagePingSchema = SchemaKey("com.snowplowanalytics.snowplow", "page_ping", "jsonschema", "1-0-0").success
    val transactionSchema = SchemaKey("com.snowplowanalytics.snowplow", "transaction", "jsonschema", "1-0-0").success
    val transactionItemSchema =
      SchemaKey("com.snowplowanalytics.snowplow", "transaction_item", "jsonschema", "1-0-0").success
    val structSchema = SchemaKey("com.google.analytics", "event", "jsonschema", "1-0-0").success
  }

  def extractSchema(event: EnrichedEvent)(implicit resolver: Resolver): Validation[String, SchemaKey] =
    event.event match {
      case "page_view" => Schemas.pageViewSchema
      case "page_ping" => Schemas.pagePingSchema
      case "struct" => Schemas.structSchema
      case "transaction" => Schemas.transactionSchema
      case "transaction_item" => Schemas.transactionItemSchema
      case "unstruct" => extractUnstructSchema(event)
      case eventType => "Unrecognized event [%s]".format(eventType).fail
    }

  private def extractUnstructSchema(event: EnrichedEvent)(implicit resolver: Resolver): Validation[String, SchemaKey] =
    Shredder.extractUnstructEvent(event) match {
      case Some(Success(List(json))) =>
        parseSchemaKey(Option(json.get("schema")))
      case _ =>
        "Unstructured event couldn't be extracted".fail
    }

  private def parseSchemaKey(node: Option[JsonNode]): Validation[String, SchemaKey] = node match {
    case Some(textNode: TextNode) =>
      SchemaKey.parse(textNode.textValue()).<-:(_.toString)
    case _ =>
      "Unrecognized unstructured event structure".fail // It's validated by the Shredder, so it should never happen
  }
}
