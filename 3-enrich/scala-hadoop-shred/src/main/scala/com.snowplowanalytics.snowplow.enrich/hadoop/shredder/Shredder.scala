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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package shredder

// Jackson
import com.fasterxml.jackson.databind.{
  JsonNode
}

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Common Enrich
import common._
import outputs.CanonicalOutput

// This project
import iglu.SchemaRepo
import hadoop.utils.{
  JsonUtils,
  ValidatableJsonNode,
  ProcessingMessageUtils
}
import ValidatableJsonNode._
import ProcessingMessageUtils._

/**
 * The shredder takes the two fields containing JSONs
 * (contexts and unstructured event properties) and
 * "shreds" their contents into a List of JsonNodes
 * ready for loading into dedicated tables in the
 * database.
 */
object Shredder {

  private val TypeHierarchyRoot = "events" // All shredded JSONs have the events table as their ultimate parent

  /**
   * Shred the CanonicalOutput's two fields which
   * contain JSONs: contexts and unstructured event
   * properties. By shredding we mean:
   *
   * 1. Verify the two fields contain valid JSONs
   * 2. Validate they conform to JSON Schema
   * 3. For the contexts, break the singular JsonNode
   *    into a List of individual context JsonNodes
   * 4. Collect the unstructured event and contexts
   *    into a singular List
   *
   * @param event The Snowplow enriched event to
   *        shred JSONs from
   * @return a Validation containing on Success a
   *         List (possible empty) of JsonNodes
   *         and on Failure a NonEmptyList of
   *         JsonNodes containing error messages
   */
  def shred(event: CanonicalOutput): ValidatedJsonList = {

    // Get our unstructured event and List of contexts, both Option-boxed
    val ue = for {
      v <- extractAndValidateJson("ue_properties", Option(event.ue_properties))
    } yield for {
      j <- v; v = List(j)
    } yield v
    
    val c  = for {
      v <- extractAndValidateJson("context", Option(event.contexts))
    } yield for {
      j <- v; v = j.iterator.toList
    } yield v

    def strip(o: Option[ValidatedJsonList]): ValidatedJsonList = o match {
      case Some(vjl) => vjl
      case None => List[JsonNode]().success
    }

    // Let's harmonize our Option[JsonNode] and Option[List[JsonNode]]
    // into a List[JsonNode], collecting Failures too
    val all = (strip(ue) |@| strip(c)) { _ ++ _ }

    // Now let's validate against the self-describing schemas
    // TODO

    // Let's define what we know so far of the type hierarchy.
    val partialHierarchy = TypeHierarchy(
      rootId     = event.event_id,
      rootTstamp = event.collector_tstamp,
      refRoot    = TypeHierarchyRoot,
      refTree    = List(TypeHierarchyRoot), // This is a partial tree. Need to complete later
      refParent  = TypeHierarchyRoot        // Hardcode as nested shredding not supported yet
    )
    // all.map

    all
  }

  /**
   * Attach a "type hierarchy" to a given JSON.
   * The hierarchy makes it possible for an
   * analyst to understand the relationship
   * between this JSON and its parent types in the
   * tree, all the way back to its root type.
   *
   * NOTE: in the future this will get more complex
   * when we support shredding of nested types
   *
   * @param instance The JSON to attach the type
   *        hierarchy to
   * @param partialHierarchy The type hierarchy to
   *        attach. Partial because we need to
   *        append the selfType to the refTree
   * @param schemaName The schema name of the JSON
   *        whose hierarchy we are describing
   * @return the JSON with type hierarchy attached
   */
  // TODO: important we need to fully populate the refTree
  private[shredder] def attachHierarchy(
    instance: JsonNode,
    partialHierarchy: TypeHierarchy,
    schemaName: String): JsonNode =
    instance // .set()

  /**
   * Extract the JSON from a String, and
   * validate it against the supplied
   * JSON Schema.
   *
   * @param field The name of the field
   *        containing the JSON instance
   * @param instance An Option-boxed JSON
   *        instance
   * @return an Option-boxed Validation
   *         containing either a Nel of
   *         JsonNodes error message on
   *         Failure, or a singular
   *         JsonNode on success
   */
  private[shredder] def extractAndValidateJson(field: String, instance: Option[String]): MaybeValidatedJson =
    for {
      i <- instance
    } yield for {
      j <- extractJson(field, i)
      v <- j.validate(true)
    } yield v

  /**
   * Wrapper around JsonUtils' extractJson which
   * converts the failure to a JsonNode Nel, for
   * compatibility with subsequent JSON Schema
   * checks.
   *
   * @param field The name of the field
   *        containing JSON
   * @param instance The JSON instance itself
   * @return the pimped ScalazArgs
   */
  private[shredder] def extractJson(field: String, instance: String): ValidatedJson =
    JsonUtils.extractJson(field, instance).toProcessingMessageNel
}
