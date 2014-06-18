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

// Scala
import scala.collection.immutable.HashMap

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
 * Companion which holds a constructor
 * for the EnrichmentConfigRegistry.
 */
object EnrichmentConfigRegistry {

  /**
   * Constructs our EnrichmentConfigRegistry
   * from the supplied JSON JValue.
   *
   * TODO: rest of docstring 
   */
  def parse(node: JValue): ValidationNel[String, EnrichmentConfigRegistry] =  {
    
    // Break into individual enrichment configs
    val enrichmentJsons: ValidationNel[String, List[JValue]] = (field[List[JValue]]("data")(node)).leftMap(
      _.map(_.toString)
      )

    // Loop through and for each:


    // 1. Check it validates against its own schema
    // 2. Identify the name of this enrichment config
    // 3. If the enrichment config is one of the ones
    //    we know how to parse, then:
    //    3.1 Check that the schemaKey for the given
    //        config matches the one we expect
    //    3.2 Use the companion parse to attempt to
    //        construct the config
    //    3.3 Collect the results and build a HashMap
    //        from the output (or Failure)

    // Validate that each of the enrichments passes its own schema

    // Loop through, and handle specific enrichments we know how to construct

      // do nothing

    NonEmptyList("OH NO").fail
  }

  /**
   * TODO: desc
   */
  private def buildEnrichmentConfig(enrichmentConfig: JValue, schemaKey: SchemaKey): ValidationNel[String, Option[Tuple2[String, EnrichmentConfig]]] = {

    val name = ScalazJson4sUtils.extractString(enrichmentConfig, NonEmptyList("name")).toValidationNel
    name.flatMap( nm => {

      if (nm == "ip_to_geo") {
        IpToGeoEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
      } else if (nm == "anon_ip") {
        AnonIpEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
      } else {
        None.success
      }
    })
  }

}

/**
 * A registry to hold all of our enrichment
 * configurations.
 *
 * In the future this may evolve to holding
 * all of our enrichments themselves.
 */
case class EnrichmentConfigRegistry(private val configs: HashMap[String, EnrichmentConfig]) {

  /**
   * Tells us if this enrichment is enabled
   * or not. An enabled enrichment will be
   * present in the HashMap of configs.
   *
   * TODO rest of docstring
   */
  def isEnabled(enrichmentName: String): Boolean =
    configs.isDefinedAt(enrichmentName)
}
