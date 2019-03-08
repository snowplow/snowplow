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
package com.snowplowanalytics.snowplow.enrich.common.utils

import java.io.IOException

import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jackson.JsonLoader
import com.snowplowanalytics.iglu.client.{SchemaKey, Validated}
import com.snowplowanalytics.iglu.client.repositories.{RepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.iglu.client.utils.{ValidationExceptions => VE}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import scalaz.Scalaz._

/**
 * Iglu repository ref that looks up a schema in test resources.
 */
case class TestResourcesRepositoryRef(override val config: RepositoryRefConfig, path: String) extends RepositoryRef {

  /**
   * Prioritize searching this class of repository to ensure that tests use it
   */
  override val classPriority: Int = 1

  /**
   * Human-readable descriptor for this
   * type of repository ref.
   */
  val descriptor = "test"

  /**
   * Retrieves an IgluSchema from the Iglu Repo as
   * a JsonNode.
   *
   * @param schemaKey The SchemaKey uniquely identifies
   *        the schema in Iglu
   * @return a Validation boxing either the Schema's
   *         JsonNode on Success, or an error String
   *         on Failure
   */
  def lookupSchema(schemaKey: SchemaKey): Validated[Option[JsonNode]] = {
    val schemaPath = s"${path}/${schemaKey.toPath}"
    try {
      JsonLoader.fromPath(schemaPath).some.success
    } catch {
      case jpe: JsonParseException => // Child of IOException so match first
        s"Problem parsing ${schemaPath} as JSON in ${descriptor} Iglu repository ${config.name}: %s"
          .format(VE.stripInstanceEtc(jpe.getMessage))
          .failure
          .toProcessingMessage
      case ioe: IOException =>
        None.success // Schema not found
      case NonFatal(e) =>
        s"Unknown problem reading and parsing ${schemaPath} in ${descriptor} Iglu repository ${config.name}: ${VE
          .getThrowableMessage(e)}".failure.toProcessingMessage
    }
  }
}
