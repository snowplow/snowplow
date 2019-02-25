/*
 * Copyright (c) 2017-2019 Snowplow Analytics Ltd. All rights reserved.
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
package pii

// Scala
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

// Scala libraries
import org.json4s
import org.json4s.{DefaultFormats, Diff, JValue}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization.write
import org.json4s.Extraction.decompose

// Java
import org.apache.commons.codec.digest.DigestUtils

// Java libraries
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath}
import com.jayway.jsonpath.MapFunction

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._
import iglu.client.{SchemaCriterion, SchemaKey}

// This project
import common.ValidatedNelMessage
import common.utils.ScalazJson4sUtils.{extract, fieldExists}
import common.outputs.EnrichedEvent

/**
 * Companion object. Lets us create a PiiPseudonymizerEnrichment
 * from a JValue.
 */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {

  implicit val formats = DefaultFormats + new PiiStrategyPseudonymizeSerializer

  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "pii_enrichment_config", "jsonschema", 2, 0, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[PiiPseudonymizerEnrichment] = {
    for {
      conf <- matchesSchema(config, schemaKey)
      emitIdentificationEvent = extract[Boolean](conf, "emitEvent").toOption
        .getOrElse(false)
      piiFields    <- extract[List[JObject]](conf, "parameters", "pii").leftMap(_.getMessage)
      piiStrategy  <- extractStrategy(config)
      piiFieldList <- extractFields(piiFields)
    } yield PiiPseudonymizerEnrichment(piiFieldList, emitIdentificationEvent, piiStrategy)
  }.leftMap(_.toProcessingMessageNel)

  private[pii] def getHashFunction(strategyFunction: String): Validation[String, DigestFunction] =
    strategyFunction match {
      case "MD2"     => { DigestUtils.md2Hex(_: Array[Byte]) }.success
      case "MD5"     => { DigestUtils.md5Hex(_: Array[Byte]) }.success
      case "SHA-1"   => { DigestUtils.sha1Hex(_: Array[Byte]) }.success
      case "SHA-256" => { DigestUtils.sha256Hex(_: Array[Byte]) }.success
      case "SHA-384" => { DigestUtils.sha384Hex(_: Array[Byte]) }.success
      case "SHA-512" => { DigestUtils.sha512Hex(_: Array[Byte]) }.success
      case fName     => s"Unknown function $fName".failure
    }

  private def extractFields(piiFields: List[JObject]): Validation[String, List[PiiField]] =
    piiFields.map {
      case field: JObject =>
        if (fieldExists(field, "pojo"))
          extractString(field, "pojo", "field").flatMap(extractPiiScalarField)
        else if (fieldExists(field, "json")) extractPiiJsonField(field \ "json")
        else
          s"PII Configuration: pii field does not include 'pojo' nor 'json' fields. Got: [${compact(field)}]"
            .failure[PiiField]
      case json => s"PII Configuration: pii field does not contain an object. Got: [${compact(json)}]".failure[PiiField]
    }.sequenceU

  private def extractPiiScalarField(fieldName: String): Validation[String, PiiScalar] =
    ScalarMutators
      .get(fieldName)
      .map(PiiScalar(_).success)
      .getOrElse(s"The specified pojo field $fieldName is not supported".failure)

  private def extractPiiJsonField(jsonField: JValue): Validation[String, PiiJson] = {
    val schemaCriterion = extractString(jsonField, "schemaCriterion")
      .flatMap(sc => SchemaCriterion.parse(sc).leftMap(_.getMessage))
      .toValidationNel
    val jsonPath = extractString(jsonField, "jsonPath").toValidationNel
    val mutator = extractString(jsonField, "field")
      .flatMap(getJsonMutator)
      .toValidationNel
    val validatedNel = (mutator |@| schemaCriterion |@| jsonPath)(PiiJson.apply)
    validatedNel.leftMap(x => s"Unable to extract PII JSON: ${x.list.mkString(",")}")
  }

  private def getJsonMutator(fieldName: String): Validation[String, Mutator] =
    JsonMutators
      .get(fieldName)
      .map(_.success)
      .getOrElse(s"The specified json field $fieldName is not supported".failure)

  private def extractString(jValue: JValue, field: String, tail: String*): Validation[String, String] =
    extract[String](jValue, field, tail: _*).leftMap(_.getMessage)

  private def extractStrategy(config: JValue): Validation[String, PiiStrategyPseudonymize] =
    extract[PiiStrategyPseudonymize](config, "parameters", "strategy")
      .leftMap(_.getMessage)

  private def matchesSchema(config: JValue, schemaKey: SchemaKey): Validation[String, JValue] =
    if (supportedSchema.matches(schemaKey))
      config.success
    else
      s"Schema key $schemaKey is not supported. A '${supportedSchema.name}' enrichment must have schema '$supportedSchema'.".failure
}

/**
 * Implements a pseudonymization strategy using any algorithm known to DigestFunction
 * @param functionName string representation of the function
 * @param hashFunction the DigestFunction to apply
 * @param salt salt added to the plain string before hashing
 */
final case class PiiStrategyPseudonymize(functionName: String, hashFunction: DigestFunction, salt: String)
    extends PiiStrategy {
  val TextEncoding                                 = "UTF-8"
  override def scramble(clearText: String): String = hash(clearText + salt)
  def hash(text: String): String                   = hashFunction(text.getBytes(TextEncoding))
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are configured as PII (personally
 * identifiable information) and apply some anonymization (currently only pseudonymization) on them. Currently a single
 * strategy for all the fields is supported due to the configuration format, and there is only one implemented strategy,
 * however the enrichment supports a strategy per field.
 *
 * The user may specify two types of fields in the config `pojo` or `json`. A `pojo` field is effectively a scalar field in the
 * EnrichedEvent, whereas a `json` is a "context" formatted field and it can either contain a single value in the case of
 * unstruct_event, or an array in the case of derived_events and contexts.
 *
 * @param fieldList a list of configured PiiFields
 * @param emitIdentificationEvent whether to emit an identification event
 * @param strategy the pseudonymization strategy to use
 */
case class PiiPseudonymizerEnrichment(fieldList: List[PiiField],
                                      emitIdentificationEvent: Boolean,
                                      strategy: PiiStrategy)
    extends Enrichment {
  implicit val json4sFormats = DefaultFormats +
    new PiiModifiedFieldsSerializer +
    new PiiStrategyPseudonymizeSerializer

  private val UnstructEventSchema =
    SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
  def transformer(event: EnrichedEvent): Unit = {
    val modifiedFields: ModifiedFields = fieldList.flatMap(_.transform(event, strategy))

    event.pii =
      if (emitIdentificationEvent && modifiedFields.nonEmpty)
        write(
          ("schema" -> UnstructEventSchema) ~ ("data" -> decompose(PiiModifiedFields(modifiedFields, strategy)))
        )
      else null
  }
}

/**
 * Specifies a scalar field in POJO and the strategy that should be applied to it.
 * @param fieldMutator the field mutator where the strategy will be applied
 */
final case class PiiScalar(fieldMutator: Mutator) extends PiiField {
  override def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields) =
    if (fieldValue != null) {
      val modifiedValue = strategy.scramble(fieldValue)
      (modifiedValue, List(ScalarModifiedField(fieldMutator.fieldName, fieldValue, modifiedValue)))
    } else (null, List())
}

/**
 * Specifies a strategy to use, a field mutator where the JSON can be found in the EnrichedEvent POJO, a schema criterion to
 * discriminate which contexts to apply this strategy to, and a JSON path within the contexts where this strategy will
 * be applied (the path may correspond to multiple fields).
 *
 * @param fieldMutator the field mutator for the JSON field
 * @param schemaCriterion the schema for which the strategy will be applied
 * @param jsonPath the path where the strategy will be applied
 */
final case class PiiJson(fieldMutator: Mutator, schemaCriterion: SchemaCriterion, jsonPath: String) extends PiiField {
  implicit val json4sFormats = DefaultFormats

  override def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields) =
    if (fieldValue != null) {
      val (parsedAndSubistuted: JValue, modifiedFields: List[JsonModifiedField]) = parse(fieldValue) match {
        case JObject(jObject) => {
          val jObjectMap: Map[String, JValue] = jObject.toMap
          val contextMapped: Map[String, (JValue, List[JsonModifiedField])] =
            jObjectMap.map(mapContextTopFields(_, strategy))
          (JObject(contextMapped.mapValues(_._1).toList), contextMapped.values.map(_._2).flatten)
        }
        case x => (x, List.empty[JsonModifiedField])
      }
      val compacted = compact(render(parsedAndSubistuted))
      (compacted, modifiedFields)
    } else (null, List.empty[JsonModifiedField])

  /**
   * Map context top fields with strategy if they match.
   */
  private def mapContextTopFields(tuple: (String, json4s.JValue),
                                  strategy: PiiStrategy): (String, (JValue, List[JsonModifiedField])) = tuple match {
    case (k: String, contexts: JValue) if k == "data" =>
      (k, contexts match {
        case JArray(contexts) =>
          val updatedAndModified: List[(JValue, List[JsonModifiedField])] =
            contexts.map(getModifiedContext(_, strategy))
          (JArray(updatedAndModified.map(_._1)), updatedAndModified.map(_._2).flatten)
        case x => getModifiedContext(x, strategy)
      })
    case (k: String, x: JValue) => (k, (x, List.empty[JsonModifiedField]))
  }

  /**
   * Returns a modified context or unstruct event along with a list of modified fields.
   */
  private def getModifiedContext(jv: JValue, strategy: PiiStrategy): (JValue, List[JsonModifiedField]) = jv match {
    case JObject(context) => modifyObjectIfSchemaMatches(context, strategy)
    case x                => (x, List.empty[JsonModifiedField])
  }

  /**
   * Tests whether the schema for this event matches the schema criterion and if it does modifies it.
   */
  private def modifyObjectIfSchemaMatches(context: List[(String, json4s.JValue)],
                                          strategy: PiiStrategy): (JObject, List[JsonModifiedField]) = {
    val fieldsObj = context.toMap
    (for {
      schema              <- fieldsObj.get("schema")
      schemaStr           <- schema.extractOpt[String]
      parsedSchemaMatches <- SchemaKey.parse(schemaStr).map(schemaCriterion.matches).toOption
      data                <- fieldsObj.get("data")
      if parsedSchemaMatches
      updated = jsonPathReplace(data, strategy, schemaStr)
    } yield (JObject(fieldsObj.updated("schema", schema).updated("data", updated._1).toList), updated._2))
      .getOrElse((JObject(context), List()))
  }

  /**
   * Replaces a value in the given context data with the result of applying the strategy that value.
   */
  private def jsonPathReplace(jValue: JValue,
                              strategy: PiiStrategy,
                              schema: String): (JValue, List[JsonModifiedField]) = {
    val objectNode      = JsonMethods.mapper.valueToTree[ObjectNode](jValue)
    val documentContext = JJsonPath.using(JsonPathConf).parse(objectNode)
    val modifiedFields  = MutableList[JsonModifiedField]()
    val documentContext2 = documentContext.map(
      jsonPath,
      new ScrambleMapFunction(strategy, modifiedFields, fieldMutator.fieldName, jsonPath, schema))
    // make sure it is a structure preserving method, see #3636
    val transformedJValue            = JsonMethods.fromJsonNode(documentContext.json[JsonNode]())
    val Diff(_, erroneouslyAdded, _) = jValue diff transformedJValue
    val Diff(_, withoutCruft, _)     = erroneouslyAdded diff transformedJValue
    (withoutCruft, modifiedFields.toList)
  }
}

private final class ScrambleMapFunction(strategy: PiiStrategy,
                                        modifiedFields: MutableList[JsonModifiedField],
                                        fieldName: String,
                                        jsonPath: String,
                                        schema: String)
    extends MapFunction {
  override def map(currentValue: AnyRef, configuration: Configuration): AnyRef = currentValue match {
    case s: String =>
      val newValue = strategy.scramble(s)
      val _        = modifiedFields += JsonModifiedField(fieldName, s, newValue, jsonPath, schema)
      newValue
    case a: ArrayNode =>
      a.elements.asScala.map {
        case t: TextNode =>
          val originalValue = t.asText()
          val newValue      = strategy.scramble(originalValue)
          modifiedFields += JsonModifiedField(fieldName, originalValue, newValue, jsonPath, schema)
          newValue
        case default: AnyRef => default
      }
    case default: AnyRef => default
  }
}
