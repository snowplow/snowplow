 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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

object Shredder {

  private val schemaPattern = """.+:([a-zA-Z0-9_\.]+)/([a-zA-Z0-9_]+)/[^/]+/(.*)""".r

  /**
   * Create an Elasticsearch field name from a schema
   *
   * "iglu:com.acme/PascalCase/jsonschema/13-0-0" -> "context_com_acme_pascal_case_13"
   *
   * @param prefix "context" or "unstruct_event"
   * @param schema Schema field from an incoming JSON
   */
  def fixSchema(prefix: String, schema: String): ValidationNel[String, String] = {
    schema match {
      case schemaPattern(organization, name, schemaVer) => {

        // Split the vendor's reversed domain name using underscores rather than dots
        val snakeCaseOrganization = organization.replaceAll("""\.""", "_").toLowerCase

        // Change the name from PascalCase to snake_case if necessary
        val snakeCaseName = name.replaceAll("([^_])([A-Z])", "$1_$2").toLowerCase

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
   *    "schema": "iglu:com.snowplowanalytics.snowplow\/contexts\/jsonschema\/1-0-0",
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
   *      }
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
   *    "com_acme_duplicated_1": [{"value": 1}, {"value": 2}]
   *    "com_acme_unduplicated": [{"unique": true}]
   *  }
   */
  def parseContexts(contexts: String): ValidationNel[String, JObject] = {
    val json = parse(contexts)
    val data = json \ "data"
    /*
    data.children.map(context => (context \ "schema", context \ "data")).collect({
      case (JString(schema), innerData) if innerData != JNothing => (fixSchema("contexts", schema), innerData)
    }).groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2)))
    */
    /*json \ "data" match {
      case JArray(data) => data.map(context => (context \ "schema", context \ "data")
      case _ => "Contexts JSON data field is not an array".failNel
    }*/

    val innerContexts: ValidationNel[String, List[(String, JValue)]] =
      data.children.map(context => (context \ "schema", context \ "data")).collect({
      case (JString(schema), innerData) if innerData != JNothing => (fixSchema("contexts", schema), innerData)
    }).map(kvp => kvp match {
      case (Failure(f), innerData) => f.fail
      case (Success(s), innerData) => ((s, innerData)).successNel      
    }).sequenceU

    innerContexts.map(_.groupBy(_._1).map(pair => (pair._1, pair._2.map(_._2))))
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
   *    "com_snowplowanalytics_snowplow_link_click_1": {"key": "value"}
   *  }
   */
  def parseUnstruct(unstruct: String): ValidationNel[String, JObject] = {
    val json = parse(unstruct)
    val data = json \ "data"
    val schema = data \ "schema"
    val innerData = data \ "data"
    val fixedSchema = schema match {
      case JString(s) => fixSchema("unstruct_event", s)
      case _ => "Unstructured event JSON did not contain a stringly typed schema field".failNel
    }
    fixedSchema.map(succ => (succ, innerData))
  }
}
