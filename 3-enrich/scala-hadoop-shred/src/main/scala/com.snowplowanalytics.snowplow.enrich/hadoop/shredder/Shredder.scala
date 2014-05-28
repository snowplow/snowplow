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
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Common Enrich
import common._
import outputs.CanonicalOutput

// This project
import iglu.{
  SchemaKey,
  SchemaRepo
}
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

  private val TypeHierarchyRoot = "events" // All shredded JSONs have the events type (aka table) as their ultimate parent

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
  def shred(event: CanonicalOutput): ValidatedJsonSchemaPairList = {

    // Define what we know so far of the type hierarchy.
    val partialHierarchy = makePartialHierarchy(
      event.event_id, event.collector_tstamp)

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

    def flatten(o: Option[ValidatedJsonList]): ValidatedJsonList = o match {
      case Some(vjl) => vjl
      case None => List[JsonNode]().success
    }

    // Let's harmonize our Option[JsonNode] and Option[List[JsonNode]]
    // into a List[JsonNode], collecting Failures too
    val all = (flatten(ue) |@| flatten(c)) { _ ++ _ }

    // Let's validate the instances against their schemas, and
    // then attach metadata to the nodes
    (for {
      list <- all
    } yield for {
      node <- list
    } yield for {
      js   <- node.validateAndIdentifySchema(false)
      mj   =  attachMetadata(js, partialHierarchy)
    } yield mj).flatMap(_.sequenceU) // Swap nested List[scalaz.Validation[...]
  }

  /**
   * Convenience to make a partial TypeHierarchy.
   * Partial because we don't have the complete
   * refTree yet.
   *
   * @param rootId The ID of the root element
   * @param rootTstamp The timestamp of the root
   *        element
   * @return the partially complete TypeHierarchy
   */
  private[shredder] def makePartialHierarchy(rootId: String, rootTstamp: String): TypeHierarchy =
    TypeHierarchy(
      rootId     = rootId,
      rootTstamp = rootId,
      refRoot    = TypeHierarchyRoot,
      refTree    = List(TypeHierarchyRoot), // This is a partial tree. Need to complete later
      refParent  = TypeHierarchyRoot        // Hardcode as nested shredding not supported yet
    )

  /**
   * A Scalaz Lens to complete the refTree within
   * a TypeHierarchy object.
   */
  private[shredder] val hierarchyLens: Lens[TypeHierarchy, List[String]] =
    Lens.lensu((ph, rt) => ph.copy(refTree = ph.refTree ++ rt), _.refTree)

  /**
   * Adds shred-related metadata to the JSON.
   * There are two envelopes of metadata to
   * attach:
   *
   * 1. schema - we replace the existing schema
   *    URI string with a full schema key object
   *    containing name, vendor, format and
   *    version as separate string properties
   * 2. hierarchy - we add a new object expressing
   *    the type hierarchy for this shredded JSON
   *
   * @param instanceSchemaPair Tuple2 containing:
   *        1. The SchemaKey identifying the schema
   *           for this JSON 
   *        2. The JsonNode for this JSON
   * @param partialHierarchy The type hierarchy to
   *        attach. Partial because the refTree is
   *        still incomplete
   * @return the Tuple2, with the JSON updated to
   *         contain the full schema key, plus the
   *         now-finalized hierarchy
   */
  private[shredder] def attachMetadata(
    instanceSchemaPair: JsonSchemaPair,
    partialHierarchy: TypeHierarchy): JsonSchemaPair = {

    val (schemaKey, instance) = instanceSchemaPair

    val schemaNode = schemaKey.asJson
    val hierarchyNode = {
      val full = hierarchyLens.set(partialHierarchy, List(schemaKey.name))
      full.asJson
    }

    // This might look unsafe but we're only here
    // if this instance has been validated as a
    // self-describing JSON, i.e. we can assume the
    // below structure
    val updated = instance.asInstanceOf[ObjectNode]
    updated.replace("schema", schemaNode)
    updated.put("hierarchy", hierarchyNode)

    (schemaKey, updated)
  }

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
