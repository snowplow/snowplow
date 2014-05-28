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

    def strip(opt: Option[ValidatedJsonList]): ValidatedJsonList = opt match {
      case Some(vjl) => vjl
      case None => List[JsonNode]().success
    }

    // Let's harmonize our Option[JsonNode] and Option[List[JsonNode]]
    // into a List[JsonNode], collecting Failures too
    (strip(ue) |@| strip(c)) { _ ++ _ }
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
