 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Scala
import scala.util.matching.Regex
import scala.annotation.tailrec

/**
 * Converts unstructured events and custom contexts to a format which the Elasticsearch
 * mapper can understand
 */
object Shredder {

  private val schemaPattern = """.+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""".r

  /**
   * Create an Elasticsearch field name from a schema
   *
   * "iglu:com.acme/PascalCase/jsonschema/13-0-0" -> "context_com_acme_pascal_case_13"
   *
   * @param prefix "context" or "unstruct_event"
   * @param schema Schema field from an incoming JSON
   * @return Elasticsearch field name
   */
   // TODO: move this to shared storage/shredding utils
   // See https://github.com/snowplow/snowplow/issues/1189
  def fixSchema(prefix: String, schema: String): ValidationNel[String, String] = {
    schema match {
      case schemaPattern(organization, name, schemaVer) => {

        // Split the vendor's reversed domain name using underscores rather than dots
        val snakeCaseOrganization = organization.replaceAll("""\.""", "_").toLowerCase

        // Change the name from PascalCase to snake_case if necessary
        val snakeCaseName = name.replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase

        // Extract the schemaver version's model
        val model = schemaVer.split("-")(0)

        s"${prefix}_${snakeCaseOrganization}_${snakeCaseName}_${model}".successNel
      }
      case _ => "Schema %s does not conform to regular expression %s".format(schema, schemaPattern.toString).failNel
    }
  }

  /**
   * Convert a contexts JSON to an Elasticsearch-compatible JObject
   * For example, the JSON
   *
   *  {
   *    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
   *    "data": [
   *      {
   *        "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0",
   *        "data": {
   *          "unique": true
   *        }
   *      },
   *      {
   *        "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0",
   *        "data": {
   *          "value": 1
   *        }
   *      },
   *      {
   *        "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0",
   *        "data": {
   *          "value": 2
   *        }
   *      }
   *    ]
   *  }
   *
   * would become
   *
   *  {
   *    "context_com_acme_duplicated_1": [{"value": 1}, {"value": 2}],
   *    "context_com_acme_unduplicated_1": [{"unique": true}]
   *  }
   *
   * @param contexts Contexts JSON
   * @return Contexts JSON in an Elasticsearch-compatible format
   */
  def parseContexts(contexts: String): ValidationNel[String, JObject] = {

    /**
     * Validates and pairs up the schema and data fields without grouping the same schemas together
     *
     * For example, the JSON
     *
     *  {
     *    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
     *    "data": [
     *      {
     *        "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0",
     *        "data": {
     *          "value": 1
     *        }
     *      },
     *      {
     *        "schema": "iglu:com.acme/duplicated/jsonschema/1-0-0",
     *        "data": {
     *          "value": 2
     *        }
     *      }
     *    ]
     *  }
     *
     * would become
     *
     * [
     *   {"context_com_acme_duplicated_1": {"value": 1}},
     *   {"context_com_acme_duplicated_1": {"value": 2}}
     * ]
     *
     * @param contextJsons List of inner custom context JSONs
     * @param accumulator Custom contexts which have already been parsed
     * @return List of validated tuples containing a fixed schema string and the original data JObject
     */
    @tailrec def innerParseContexts(contextJsons: List[JValue], accumulator: List[ValidationNel[String, (String, JValue)]]):
      List[ValidationNel[String, (String, JValue)]] = {

      contextJsons match {
        case Nil => accumulator
        case head :: tail => {
          val context = head
          val innerData = context \ "data" match {
            case JNothing => "Could not extract inner data field from custom context".failNel // TODO: decide whether to enforce object type of data
            case d => d.successNel
          }
          val fixedSchema: ValidationNel[String, String] = context \ "schema" match {
            case JString(schema) => fixSchema("contexts", schema)
            case _ => "Context JSON did not contain a stringly typed schema field".failNel
          }

          val schemaDataPair = (fixedSchema |@| innerData) {_ -> _}

          innerParseContexts(tail, schemaDataPair :: accumulator)
        }
      }
    }

    val json = parse(contexts)
    val data = json \ "data"

    data match {
      case JArray(Nil) => "Custom contexts data array is empty".failNel
      case JArray(ls) => {
        val innerContexts: ValidationNel[String, List[(String, JValue)]] = innerParseContexts(ls, Nil).sequenceU

        // Group contexts with the same schema together
        innerContexts.map(_.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2))))
      }
      case _ => "Could not extract contexts data field as an array".failNel
    }

  }

  /**
   * Convert an unstructured event JSON to an Elasticsearch-compatible JObject
   * For example, the JSON
   *
   *  {
   *    "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
   *    "data": {
   *      "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
   *      "data": {
   *        "key": "value"
   *      }
   *    }
   *  }
   *
   * would become
   *
   *  {
   *    "unstruct_com_snowplowanalytics_snowplow_link_click_1": {"key": "value"}
   *  }
   *
   * @param unstruct Unstructured event JSON
   * @return Unstructured event JSON in an Elasticsearch-compatible format
   */
  def parseUnstruct(unstruct: String): ValidationNel[String, JObject] = {
    val json = parse(unstruct)
    val data = json \ "data"
    val schema = data \ "schema"
    val innerData = data \ "data" match {
      case JNothing => "Could not extract inner data field from unstructured event".failNel // TODO: decide whether to enforce object type of data
      case d => d.successNel
    }
    val fixedSchema = schema match {
      case JString(s) => fixSchema("unstruct_event", s)
      case _ => "Unstructured event JSON did not contain a stringly typed schema field".failNel
    }

    (fixedSchema |@| innerData) {_ -> _}
  }
}
