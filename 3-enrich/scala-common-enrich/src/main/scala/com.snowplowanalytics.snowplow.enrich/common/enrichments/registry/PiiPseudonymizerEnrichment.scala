/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

// Scala libraries
import org.json4s
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.DefaultFormats

// Java
import org.apache.commons.codec.digest.DigestUtils

// Java libraries
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath, Option => JOption}
import com.jayway.jsonpath.MapFunction

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._
import iglu.client.{SchemaCriterion, SchemaKey}

// This project
import common.ValidatedNelMessage
import common.utils.ScalazJson4sUtils
import common.outputs.EnrichedEvent

object PiiConstants {
  type DigestFunction = Function1[Array[Byte], String]
  type Mutator        = (EnrichedEvent, String => String) => Unit

  /**
   * This and the next constant maps from a config field name to an EnrichedEvent mutator. The structure is such so that
   * it preserves type safety, and it can be easily replaced in the future by generated code that will use the config as
   * input.
   */
  val ScalarMutators: Map[String, Mutator] = Map(
    "user_id" -> { (event: EnrichedEvent, fn: String => String) =>
      event.user_id = fn(event.user_id)
    },
    "user_ipaddress" -> { (event: EnrichedEvent, fn: String => String) =>
      event.user_ipaddress = fn(event.user_ipaddress)
    },
    "user_fingerprint" -> { (event: EnrichedEvent, fn: String => String) =>
      event.user_fingerprint = fn(event.user_fingerprint)
    },
    "domain_userid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.domain_userid = fn(event.domain_userid)
    },
    "network_userid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.network_userid = fn(event.network_userid)
    },
    "ip_organization" -> { (event: EnrichedEvent, fn: String => String) =>
      event.ip_organization = fn(event.ip_organization)
    },
    "ip_domain" -> { (event: EnrichedEvent, fn: String => String) =>
      event.ip_domain = fn(event.ip_domain)
    },
    "tr_orderid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.tr_orderid = fn(event.tr_orderid)
    },
    "ti_orderid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.ti_orderid = fn(event.ti_orderid)
    },
    "mkt_term" -> { (event: EnrichedEvent, fn: String => String) =>
      event.mkt_term = fn(event.mkt_term)
    },
    "mkt_content" -> { (event: EnrichedEvent, fn: String => String) =>
      event.mkt_content = fn(event.mkt_content)
    },
    "se_category" -> { (event: EnrichedEvent, fn: String => String) =>
      event.se_category = fn(event.se_category)
    },
    "se_action" -> { (event: EnrichedEvent, fn: String => String) =>
      event.se_action = fn(event.se_action)
    },
    "se_label" -> { (event: EnrichedEvent, fn: String => String) =>
      event.se_label = fn(event.se_label)
    },
    "se_property" -> { (event: EnrichedEvent, fn: String => String) =>
      event.se_property = fn(event.se_property)
    },
    "mkt_clickid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.mkt_clickid = fn(event.mkt_clickid)
    },
    "refr_domain_userid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.refr_domain_userid = fn(event.refr_domain_userid)
    },
    "domain_sessionid" -> { (event: EnrichedEvent, fn: String => String) =>
      event.domain_sessionid = fn(event.domain_sessionid)
    }
  )

  val JsonMutators: Map[String, Mutator] = Map(
    "contexts" -> { (event: EnrichedEvent, fn: String => String) =>
      event.contexts = fn(event.contexts)
    },
    "derived_contexts" -> { (event: EnrichedEvent, fn: String => String) =>
      event.derived_contexts = fn(event.derived_contexts)
    },
    "unstruct_event" -> { (event: EnrichedEvent, fn: String => String) =>
      event.unstruct_event = fn(event.unstruct_event)
    }
  )
}

/**
 * PiiField trait. This corresponds to a configuration top-level field (i.e. either a scalar or a JSON field) along with
 * a function to apply that strategy to the EnrichedEvent POJO (A scalar field is represented in config py "pojo")
 */
sealed trait PiiField {
  import PiiConstants.Mutator

  /**
   * Strategy for this field
   *
   * @return PiiStrategy a strategy to be applied to this field
   */
  def strategy: PiiStrategy

  /**
   * The POJO mutator for this field
   *
   * @return fieldMutator
   */
  def fieldMutator: Mutator

  /**
   * Gets an enriched event from the enrichment manager and modifies it according to the specified strategy.
   *
   * @param event The enriched event
   */
  def transform(event: EnrichedEvent): Unit = fieldMutator(event, applyStrategy)

  protected def applyStrategy(fieldValue: String): String
}

/**
 * PiiStrategy trait. This corresponds to a strategy to apply to a single field. Currently only String input is
 * supported.
 */
sealed trait PiiStrategy {
  def scramble(clearText: String): String
}

/**
 * Companion object. Lets us create a PiiPseudonymizerEnrichment
 * from a JValue.
 */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {
  import PiiConstants._

  implicit val json4sFormats = DefaultFormats

  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "pii_enrichment_config", "jsonschema", 1, 0, 0)

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

  private def getHashFunction(strategyFunction: String): Validation[String, DigestFunction] =
    strategyFunction match {
      case "MD2" => { (b: Array[Byte]) =>
        DigestUtils.md2Hex(b)
      }.success
      case "MD5" => { (b: Array[Byte]) =>
        DigestUtils.md5Hex(b)
      }.success
      case "SHA-1" => { (b: Array[Byte]) =>
        DigestUtils.sha1Hex(b)
      }.success
      case "SHA-256" => { (b: Array[Byte]) =>
        DigestUtils.sha256Hex(b)
      }.success
      case "SHA-384" => { (b: Array[Byte]) =>
        DigestUtils.sha384Hex(b)
      }.success
      case "SHA-512" => { (b: Array[Byte]) =>
        DigestUtils.sha512Hex(b)
      }.success
      case fName => s"Unknown function $fName".failure
    }

  private def extractFields(piiFields: List[JObject], strategy: PiiStrategy): Validation[String, List[PiiField]] =
    piiFields.map {
      case field: JObject =>
        if (ScalazJson4sUtils.fieldExists(field, "pojo"))
          extractString(field, "pojo", "field").flatMap(extractPiiScalarField(strategy, _))
        else if (ScalazJson4sUtils.fieldExists(field, "json")) extractPiiJsonField(strategy, field \ "json")
        else s"PII Configuration: pii field does not include 'pojo' nor 'json' fields. Got: [${compact(field)}]".failure
      case json => s"PII Configuration: pii field does not contain an object. Got: [${compact(json)}]".failure
    }.sequenceU

  private def extractPiiScalarField(strategy: PiiStrategy, fieldName: String): Validation[String, PiiScalar] =
    ScalarMutators
      .get(fieldName)
      .map(PiiScalar(strategy, _).success)
      .getOrElse(s"The specified pojo field ${fieldName} is not supported".failure)

  private def extractPiiJsonField(strategy: PiiStrategy, jsonField: JValue): Validation[String, PiiJson] =
    (extractString(jsonField, "field")
      .flatMap(
        fieldName =>
          JsonMutators
            .get(fieldName)
            .map(_.success)
            .getOrElse(s"The specified json field ${compact(jsonField)} is not supported".failure)) |@|
      extractString(jsonField, "schemaCriterion").flatMap(sc => SchemaCriterion.parse(sc).leftMap(_.getMessage)) |@|
      extractString(jsonField, "jsonPath")) { (fieldMutator: Mutator, sc: SchemaCriterion, jsonPath: String) =>
      PiiJson(strategy, fieldMutator, sc, jsonPath)
    }

  private def extractString(jValue: JValue, field: String, tail: String*): Validation[String, String] =
    ScalazJson4sUtils.extract[String](jValue, field, tail: _*).leftMap(_.getMessage)

  private def extractStrategyFunction(config: JValue): Validation[String, String] =
    ScalazJson4sUtils
      .extract[String](config, "parameters", "strategy", "pseudonymize", "hashFunction")
      .leftMap(_.getMessage)

  private def matchesSchema(config: JValue, schemaKey: SchemaKey): Validation[String, JValue] =
    if (supportedSchema.matches(schemaKey)) {
      config.success
    } else {
      "Schema key %s is not supported. A '%s' enrichment must have schema '%s'."
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .failure
    }
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are configured as PII (personally
 * identifiable information) and apply some anonymization (currently only pseudonymization) on them. Currently a single
 * strategy for all the fields is supported due to the config format, and there is only one implemented strategy,
 * however the enrichment supports a strategy per field.
 *
 * The user may specify two types of fields POJO or JSON. A POJO field is effectively a scalar field in the
 * EnrichedEvent, whereas a JSON is a "context" formatted field and it can be wither a scalar in the case of
 * unstruct_event or an array in the case of derived_events and contexts
 *
 * @param fieldList a list of configured PiiFields
 */
case class PiiPseudonymizerEnrichment(fieldList: List[PiiField]) extends Enrichment {
  def transformer(event: EnrichedEvent): Unit = fieldList.foreach(_.transform(event))
}

/**
 * Specifies a scalar field in POJO and the strategy that should be applied to it.
 * @param strategy the strategy that should be applied
 * @param fieldMutator the field mutator where the strategy will be applied
 */
final case class PiiScalar(strategy: PiiStrategy, fieldMutator: PiiConstants.Mutator) extends PiiField {
  override def applyStrategy(fieldValue: String): String =
    if (fieldValue != null) strategy.scramble(fieldValue) else null
}

/**
 * Specifies a strategy to use, a field mutator where the JSON can be found in the EnrichedEvent POJO, a schema criterion to
 * discriminate which contexts to apply this strategy to, and a json path within the contexts where this strategy will
 * be applied (the path may correspond to multiple fields).
 *
 * @param strategy the strategy that should be applied
 * @param fieldMutator the field mutator for the json field
 * @param schemaCriterion the schema for which the strategy will be applied
 * @param jsonPath the path where the strategy will be applied
 */
final case class PiiJson(strategy: PiiStrategy,
                         fieldMutator: PiiConstants.Mutator,
                         schemaCriterion: SchemaCriterion,
                         jsonPath: String)
    extends PiiField {
  implicit val json4sFormats = DefaultFormats

  override def applyStrategy(fieldValue: String): String =
    if (fieldValue != null) {
      compact(render(parse(fieldValue) match {
        case JObject(jObject) => {
          val jObjectMap = jObject.toMap
          val updated = jObjectMap.filterKeys(_ == "data").mapValues {
            case JArray(contexts) =>
              JArray(contexts.map {
                case JObject(context) => modifyObjectIfSchemaMatches(context)
                case x                => x
              })
            case JObject(unstructEvent) => modifyObjectIfSchemaMatches(unstructEvent)
            case x                      => x
          }
          JObject((jObjectMap ++ updated).toList)
        }
        case x => x
      }))
    } else null

  private def modifyObjectIfSchemaMatches(context: List[(String, json4s.JValue)]): JObject = {
    val fieldsObj = context.toMap
    (for {
      schema              <- fieldsObj.get("schema")
      parsedSchemaMatches <- SchemaKey.parse(schema.extract[String]).map(schemaCriterion.matches).toOption
      data                <- fieldsObj.get("data")
      if parsedSchemaMatches
    } yield JObject(fieldsObj.updated("schema", schema).updated("data", jsonPathReplace(data)).toList))
      .getOrElse(JObject(context))
  }

  // Configuration for JsonPath
  private val JacksonNodeJsonObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper
  }
  private val JsonPathConf =
    Configuration
      .builder()
      .options(JOption.SUPPRESS_EXCEPTIONS)
      .jsonProvider(new JacksonJsonNodeJsonProvider(JacksonNodeJsonObjectMapper))
      .build()

  /**
   * Replaces a value in the given context data with the result of applying the strategy that value.
   */
  private def jsonPathReplace(jValue: JValue): JValue = {
    val objectNode      = JsonMethods.mapper.valueToTree[ObjectNode](jValue)
    val documentContext = JJsonPath.using(JsonPathConf).parse(objectNode)
    documentContext.map(
      jsonPath,
      ScrambleMapFunction(strategy)
    )
    JsonMethods.fromJsonNode(documentContext.json[JsonNode]())
  }
}

case class ScrambleMapFunction(val strategy: PiiStrategy) extends MapFunction {
  override def map(currentValue: AnyRef, configuration: Configuration): AnyRef = currentValue match {
    case s: String => strategy.scramble(s)
    case a: ArrayNode =>
      a.elements.asScala.map {
        case t: TextNode     => strategy.scramble(t.asText())
        case default: AnyRef => default
      }
    case default: AnyRef => default
  }
}

/**
 * Implements a pseudonymization strategy using any algorithm known to DigestFunction
 * @param hashFunction the DigestFunction to apply
 */
case class PiiStrategyPseudonymize(hashFunction: PiiConstants.DigestFunction) extends PiiStrategy {
  val TextEncoding                                 = "UTF-8"
  override def scramble(clearText: String): String = hash(clearText)
  def hash(text: String): String                   = hashFunction(text.getBytes(TextEncoding))
}
