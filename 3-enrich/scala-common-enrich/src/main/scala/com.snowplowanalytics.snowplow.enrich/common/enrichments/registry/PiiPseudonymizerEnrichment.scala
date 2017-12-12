/*
 * Copyright (c) 2017-2018 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow.enrich
package common.enrichments.registry

// Scala
import scala.collection.JavaConverters._

// Snowplow
import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.MapFunction
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedNelMessage
import com.snowplowanalytics.snowplow.enrich.common.utils.MapTransformer.TransformMap
import com.snowplowanalytics.snowplow.enrich.common.utils.ScalazJson4sUtils

// Iglu
import iglu.client.validation.ProcessingMessageMethods._

// Scala libraries
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.DefaultFormats
import org.json4s.MappingException

// Java
import java.security.{MessageDigest, NoSuchAlgorithmException}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath, Option => JOption}

// Scalaz
import scalaz._
import Scalaz._

/**
 * PiiField trait. This corresponds to a configuration top-level field (i.e. either a POJO or a JSON field) along with
 * a function to apply that strategy to a TransformMap
 *
 * Members are:
 * strategy: the strategy to be used in this field
 * fieldName: the fieldName where this startegy will be applied
 * transformer: a function that modifies the TransformMap by adding PII transformations where appropriate
 */
sealed trait PiiField {
  def strategy: PiiStrategy
  def fieldName: String
  protected def applyStrategy(fieldValue: String): String
  def transformer(transformMap: TransformMap) =
    transformMap.collect {
      case (inputField: String, (tf: Function2[String, String, Validation[String, String]], outputField: String))
          if (outputField == fieldName) =>
        (inputField, ((arg1: String, arg2: String) => tf.tupled.andThen(_.map(applyStrategy))((arg1, arg2)), outputField))
    }
}

/**
 * PiiStrategy trait. This corresponds to a strategy to apply to a single field. Currently only only String input is
 * supported.
 */
sealed trait PiiStrategy {
  def scramble: String => String
}

/**
 * Companion object. Lets us create a PiiPseudonymizerEnrichment
 * from a JValue.
 */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {
  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "pii_enrichment_config", "jsonscehma", 1, 0, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[PiiPseudonymizerEnrichment] = {
    for {
      conf <- matchesSchema(config, schemaKey)
      enabled = ScalazJson4sUtils.extract[Boolean](conf, "enabled").toOption.getOrElse(false)
      piiFields        <- ScalazJson4sUtils.extract[List[JObject]](conf, "parameters", "pii").leftMap(_.getMessage)
      strategyFunction <- extractStrategyFunction(config)
      hashFunction     <- getHashFunction(strategyFunction)
      piiFieldList     <- extractFields(piiFields, PiiStrategyPseudonymize(hashFunction))
    } yield if (enabled) PiiPseudonymizerEnrichment(piiFieldList) else PiiPseudonymizerEnrichment(List())
  }.leftMap(_.toProcessingMessageNel)

  private def getHashFunction(strategyFunction: String) =
    try {
      MessageDigest.getInstance(strategyFunction).success
    } catch {
      case e: NoSuchAlgorithmException =>
        s"Could not parse PII enrichment config: ${e.getMessage()}".fail
    }

  private def extractFields(piiFields: List[JObject], strategy: PiiStrategy) =
    piiFields.map {
      case JObject(List(("pojo", JObject(List(("field", JString(fieldName))))))) => PiiPojo(strategy, fieldName).success
      case JObject(List(("json", jsonField))) =>
        val fieldsDisj: \/[MappingException, List[String]] = List(extractString(jsonField, "field"),
                                                                  extractString(jsonField, "schemaCriterion"),
                                                                  extractString(jsonField, "jsonPath")).sequenceU
        fieldsDisj.validation
          .leftMap(_ => "Configuration file has unexpected structure")
          .flatMap[String, PiiJson] { (l: List[String]) =>
            SchemaCriterion
              .parse(l(1))
              .rightMap { sc =>
                PiiJson(strategy, l(0), sc, l(2))
              }
              .leftMap(_.getMessage)
          }
      case x => "Configuration file has unexpected structure".failure
    }.sequenceU

  implicit val json4sFormats = DefaultFormats

  private def extractString(jValue: JValue, field: String): \/[MappingException, String] =
    \/.fromTryCatchThrowable[String, MappingException] {
      (jValue \ field).extract[String]
    }

  private def extractStrategyFunction(config: JValue) =
    ScalazJson4sUtils
      .extract[String](config, "parameters", "strategy", "pseudonymize", "hashFunction")
      .leftMap(_.getMessage)

  private def matchesSchema(config: JValue, schemaKey: SchemaKey): Validation[String, JValue] =
    if (supportedSchema.matches(schemaKey)) {
      config.success
    } else {
      ("Schema key %s is not supported. A '%s' enrichment must have schema '%s'.")
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .failure
    }
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are configured as PII and apply
 * some anonymization (currently only psudonymizantion) on them. Currently a single strategy for all the fields is
 * supported due to the config format, and there is only one implemented strategy, however the enrichment supports a
 * strategy per field configuration.
 *
 * The user may specify two types of fields POJO or JSON. A POJO field is effectively a scalar field in the
 * EnrichedEvent, whereas a JSON is a "context" formatted field (a JSON string in "contexts" field in enriched event)
 *
 * @param fieldList a lits of configured PiiFields
 */
case class PiiPseudonymizerEnrichment(fieldList: List[PiiField]) extends Enrichment {
  def transformer(transformMap: TransformMap): TransformMap = transformMap ++ fieldList.map(_.transformer(transformMap)).reduce(_ ++ _)
}

/**
 * Specifies a field in POJO and the strategy that should be applied to it.
 * @param strategy the strategy that should be applied
 * @param fieldName the field where the strategy will be applied
 */
final case class PiiPojo(strategy: PiiStrategy, fieldName: String) extends PiiField {
  override def applyStrategy(fieldValue: String): String = strategy.scramble(fieldValue)
}

/**
 * Specifies a strategy to use, a field (should be "contexts") where the JSON can be found, a schema criterion to
 * discriminate which contexts to apply this strategy to, and a json path within the contexts where this strategy will
 * be apllied (the path may correspond to multiple fields).
 *
 * @param strategy the strategy that should be applied
 * @param fieldName the field in POJO where the json is to be found
 * @param schemaCriterion the schema for which the strategy will be applied
 * @param jsonPath the path where the strategy will be applied
 */
final case class PiiJson(strategy: PiiStrategy, fieldName: String, schemaCriterion: SchemaCriterion, jsonPath: String) extends PiiField {
  implicit val json4sFormats = DefaultFormats

  override def applyStrategy(fieldValue: String): String =
    compact(render(parse(fieldValue).transformField {
      case JField("data", contents) =>
        ("data", contents.transform {
          case ja: JArray =>
            ja.transform {
              case JObject(context) =>
                val fields: List[JValue] = List("schema", "data").flatMap(context.toMap.get)
                if (fields.size == 2 && SchemaKey.parse(fields(0).extract[String]).map(schemaCriterion.matches(_)).getOrElse(false))
                  JObject(List(("schema", fields(0)), ("data", jsonPathReplace(fields(1)))))
                else JObject(context)
            }
        })
    }))

  //Configuration for JsonPath
  private val jsonPathConf =
    Configuration.builder().options(JOption.SUPPRESS_EXCEPTIONS).jsonProvider(new JacksonJsonNodeJsonProvider()).build()

  /**
   * Replaces a value in the given context data with the result of applying the strategy that value.
   *
   */
  private def jsonPathReplace(jValue: JValue): JValue = {
    val objectNode      = JsonMethods.mapper.valueToTree[ObjectNode](jValue)
    val documentContext = JJsonPath.using(jsonPathConf).parse(objectNode)
    documentContext.map(
      jsonPath,
      new MapFunction {
        override def map(currentValue: scala.Any, configuration: Configuration): AnyRef = currentValue match {
          case s: String => strategy.scramble(s)
          case a: ArrayNode =>
            a.elements.asScala.map {
              case t: TextNode     => strategy.scramble(t.asText())
              case default: AnyRef => default
            }
          case default: AnyRef => default
        }
      }
    )
    JsonMethods.fromJsonNode(documentContext.json[JsonNode]())
  }
}

/**
 * Implements a pseudonymization strategy using any algorithm known to MessageDigest
 * @param hashFunction the MessageDigest function to apply
 */
case class PiiStrategyPseudonymize(hashFunction: MessageDigest) extends PiiStrategy {
  val TextEncoding               = "UTF-8"
  override val scramble          = (clearText: String) => hash(clearText)
  def hash(text: String): String = String.format("%064x", new java.math.BigInteger(1, hashFunction.digest(text.getBytes(TextEncoding))))
}
