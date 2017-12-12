package com.snowplowanalytics.snowplow.enrich.common.utils

// Java
import java.io.IOException

import scala.util.control.NonFatal

// Jackson
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jackson.JsonLoader

// Scalaz
import scalaz.Scalaz._

//Snowplow
import com.snowplowanalytics.iglu.client.repositories.{RepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.iglu.client.{SchemaKey,                  Validated, utils, validation}

// This project
import com.snowplowanalytics.iglu.client.utils.{ValidationExceptions => VE}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

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
        s"Unknown problem reading and parsing ${schemaPath} in ${descriptor} Iglu repository ${config.name}: ${VE.getThrowableMessage(e)}".failure.toProcessingMessage
    }
  }
}
