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

  def fixSchema(prefix: String, schema: String): ValidationNel[String, String] = {
    schema match {
      case schemaPattern(organization, name, schemaVer) => {
        val snakeCaseOrganization = organization.replaceAll("""\.""", "_").toLowerCase
        val snakeCaseName = name.replaceAll("([^_])([A-Z])", "$1_$2").toLowerCase
        val model = schemaVer.split("-")(0)
        s"${prefix}_${snakeCaseOrganization}_${snakeCaseName}_${model}".successNel
      }
      case _ => "Schema %s does not conform to regular expression %s".format(schema, schemaPattern.toString).failNel
    }
  }

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

  def parseUnstruct(unstruct: String): ValidationNel[String, JObject] = {
    val json = parse(unstruct)
    val data = json \ "data"
    val schema = data \ "schema"
    val innerData = data \ "data"
    val fixedSchema = schema match {
      case JString(s) => fixSchema("unstruct_event", s)
      case _ => "Unstructured event JSON did not contain a string schema field".failNel
    }
    fixedSchema.map(succ => (succ, innerData))
  }
}
