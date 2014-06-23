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
package com.snowplowanalytics
package snowplow
package enrich
package common
package config

import utils.ScalazJson4sUtils
import enrichments.{
  AnonIpEnrichment,
  IpToGeoEnrichment
}

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client._
import iglu.client.validation.ValidatableJsonMethods._
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

/**
 * Companion which holds a constructor
 * for the EnrichmentConfigRegistry.
 */
object EnrichmentConfigRegistry {

  private val EnrichmentConfigSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", "1-0-0")

  /**
   * Constructs our EnrichmentConfigRegistry
   * from the supplied JSON JValue.
   *
   * TODO: rest of docstring 
   */
  def parse(node: JValue)(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentConfigRegistry] =  {

    val configs: ValidatedNelMessage[JValue] = asJsonNode(node).validateAndIdentifySchema(dataOnly = true)
      .flatMap( s =>
        if (s._1 != EnrichmentConfigSchemaKey) {
          "Oh no, I only know how to handle enrichments 1-0-0".toProcessingMessage.failNel
        } else {
          fromJsonNode(s._2).success
        })

    // Break into individual enrichment configs
    // TODO fix this
    val enrichmentJsons: ValidatedNelMessage[List[JValue]] = (field[List[JValue]]("data")(node)).leftMap(
      _.map(_.toString.toProcessingMessage)
      )

    // Loop through and for each:

    // 1. Check it validates against its own schema
    val validatedEnrichmentJsonTuples: ValidatedNelMessage[List[JsonSchemaPair]] = (for {
      jsons <- enrichmentJsons // <- Success
    } yield for {    
      json  <- jsons           // <- List
    } yield for {
      valid <- asJsonNode(json).validateAndIdentifySchema(dataOnly = true)
    } yield valid).flatMap(_.sequenceU) // Swap nested List[scalaz.Validation[...]

    // 2. Identify the name of this enrichment config
    // 3. If the enrichment config is one of the ones
    //    we know how to parse, then:
    //    3.1 Check that the schemaKey for the given
    //        config matches the one we expect
    //    3.2 Use the companion parse to attempt to
    //        construct the config
    val configTuples: ValidatedNelMessage[List[(String, EnrichmentConfig)]] = (for {
        tuples <- validatedEnrichmentJsonTuples // <- Success
      } yield for {
        tuple <- tuples                         // <- List
      } yield for {
        result <- buildEnrichmentConfig(fromJsonNode(tuple._2), tuple._1)
      } yield result)
      .flatMap(_.sequenceU) // Explain what this is doing
      .map(_.flatten)       // Eliminate our Option boxing (drop Nones)

    //    3.3 Collect the results and build a Map
    //        from the output (or Failure)
    val enrichmentsMap: ValidatedNelMessage[Map[String, EnrichmentConfig]] = configTuples.map(_.toMap)

    // 4 Build an EnrichmentConfigRegistry from the Map
    enrichmentsMap.bimap(
      e => NonEmptyList(e.toString.toProcessingMessage),
      s => EnrichmentConfigRegistry(s)
      )
  }

  /**
   * Builds an EnrichmentConfig from a JValue if it has a 
   * recognized name field and matches a schema key 
   *
   * @param enrichmentConfig JValue with enrichment information
   * @param schemaKey SchemaKey for the JValue
   * @return ValidatedNelMessage boxing Option boxing Tuple2 containing
   *         the EnrichmentConfig object and the schemaKey
   */
  private def buildEnrichmentConfig(enrichmentConfig: JValue, schemaKey: SchemaKey): ValidatedNelMessage[Option[Tuple2[String, EnrichmentConfig]]] = {

    val name: ValidatedNelMessage[String] = ScalazJson4sUtils.extractString(enrichmentConfig, NonEmptyList("name")).toValidationNel
    name.flatMap( nm => {

      if (nm == "ip_to_geo") {
        IpToGeoEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
      } else if (nm == "anon_ip") {
        AnonIpEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
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
case class EnrichmentConfigRegistry(private val configs: Map[String, EnrichmentConfig]) {

  /**
   * Returns an Option boxing the AnonIpEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the AnonIpEnrichment instance
   */
  def getAnonIpEnrichment: Option[AnonIpEnrichment] =
    getEnrichment[AnonIpEnrichment]("anon_ip")

  /**
   * Returns an Option boxing the IpToGeoEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the IpToGeoEnrichment instance
   */
  def getIpToGeoEnrichment: Option[IpToGeoEnrichment] = 
    getEnrichment[IpToGeoEnrichment]("ip_to_geo")

  /**
   * Returns an Option boxing an Enrichment
   * config value if present, or None if not
   *
   * @tparam A Expected type of the enrichment to get
   * @param name The name of the enrichment to get
   * @return Option boxing the enrichment
   */
  private def getEnrichment[A <: EnrichmentConfig : Manifest](name: String): Option[A] =
    configs.get(name).map(cast[A](_))

  // Adapted from http://stackoverflow.com/questions/6686992/scala-asinstanceof-with-parameterized-types
  private def cast[A <: AnyRef : Manifest](a : Any) : A 
    = manifest.runtimeClass.cast(a).asInstanceOf[A]
}
