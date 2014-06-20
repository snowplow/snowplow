/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package config

import utils.ScalazJson4sUtils

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client._

/**
 * Trait inherited by every enrichment config case class
 */
trait EnrichmentConfig

/**
 * Trait to hold helpers relating to enrichment config
 */
trait EnrichmentConfigParseable {

  val supportedSchemaKey: SchemaKey

  /**
   * Tests whether a JSON is parseable by a
   * specific EnrichmentConfig constructor
   *
   * @param config The JSON
   * @param schemaKey The schemaKey which needs
   * to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: JValue, schemaKey: SchemaKey): ValidationNel[String, JValue] = {
    if (schemaKey == supportedSchemaKey) {
      config.success
    } else {
      ("Wrong type of JSON for an enrichment of type %").format(schemaKey.name).fail.toValidationNel
    }
  }

  /**
   * Shortcut for getting a value from within
   * the "parameters" field of a JSON
   *
   * @param property The name of the field
   * @return NonEmptyList of the inner value
   */
  def parameter(property: String) = NonEmptyList("parameters", property)
}
