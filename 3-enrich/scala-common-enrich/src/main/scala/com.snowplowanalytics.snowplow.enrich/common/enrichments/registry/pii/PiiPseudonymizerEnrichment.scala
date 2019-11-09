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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry
package pii

import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import cats.data.ValidatedNel
import cats.implicits._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}

import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath}
import com.jayway.jsonpath.MapFunction

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}

import io.circe._
import io.circe.jackson._
import io.circe.syntax._

import org.apache.commons.codec.digest.DigestUtils

import adapters.registry.Adapter
import outputs.EnrichedEvent
import serializers._
import utils.CirceUtils

/** Companion object. Lets us create a PiiPseudonymizerEnrichment from a Json. */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {

  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "pii_enrichment_config",
      "jsonschema",
      2,
      0,
      0
    )

  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, PiiPseudonymizerConf] = {
    for {
      conf <- matchesSchema(config, schemaKey)
      emitIdentificationEvent = CirceUtils
        .extract[Boolean](conf, "emitEvent")
        .toOption
        .getOrElse(false)
      piiFields <- CirceUtils
        .extract[List[Json]](conf, "parameters", "pii")
        .toEither
      piiStrategy <- CirceUtils
        .extract[PiiStrategyPseudonymize](config, "parameters", "strategy")
        .toEither
      piiFieldList <- extractFields(piiFields)
    } yield PiiPseudonymizerConf(piiFieldList, emitIdentificationEvent, piiStrategy)
  }.toValidatedNel

  private[pii] def getHashFunction(strategyFunction: String): Either[String, DigestFunction] =
    strategyFunction match {
      case "MD2" => { DigestUtils.md2Hex(_: Array[Byte]) }.asRight
      case "MD5" => { DigestUtils.md5Hex(_: Array[Byte]) }.asRight
      case "SHA-1" => { DigestUtils.sha1Hex(_: Array[Byte]) }.asRight
      case "SHA-256" => { DigestUtils.sha256Hex(_: Array[Byte]) }.asRight
      case "SHA-384" => { DigestUtils.sha384Hex(_: Array[Byte]) }.asRight
      case "SHA-512" => { DigestUtils.sha512Hex(_: Array[Byte]) }.asRight
      case fName => s"Unknown function $fName".asLeft
    }

  private def extractFields(piiFields: List[Json]): Either[String, List[PiiField]] =
    piiFields.map { json =>
      CirceUtils
        .extract[String](json, "pojo", "field")
        .toEither
        .flatMap(extractPiiScalarField)
        .orElse {
          json.hcursor
            .downField("json")
            .focus
            .toRight("No json field")
            .flatMap(extractPiiJsonField)
        }
        .orElse {
          ("PII Configuration: pii field does not include 'pojo' nor 'json' fields. " +
            s"Got: [${json.noSpaces}]").asLeft
        }
    }.sequence

  private def extractPiiScalarField(fieldName: String): Either[String, PiiScalar] =
    ScalarMutators
      .get(fieldName)
      .map(PiiScalar(_).asRight)
      .getOrElse(s"The specified pojo field $fieldName is not supported".asLeft)

  private def extractPiiJsonField(jsonField: Json): Either[String, PiiJson] = {
    val schemaCriterion = CirceUtils
      .extract[String](jsonField, "schemaCriterion")
      .toEither
      .flatMap(sc => SchemaCriterion.parse(sc).toRight(s"Could not parse schema criterion $sc"))
      .toValidatedNel
    val jsonPath = CirceUtils.extract[String](jsonField, "jsonPath").toValidatedNel
    val mutator = CirceUtils
      .extract[String](jsonField, "field")
      .toEither
      .flatMap(getJsonMutator)
      .toValidatedNel
    (mutator, schemaCriterion, jsonPath)
      .mapN(PiiJson.apply)
      .leftMap(x => s"Unable to extract PII JSON: ${x.toList.mkString(",")}")
      .toEither
  }

  private def getJsonMutator(fieldName: String): Either[String, Mutator] =
    JsonMutators
      .get(fieldName)
      .map(_.asRight)
      .getOrElse(s"The specified json field $fieldName is not supported".asLeft)

  private def matchesSchema(config: Json, schemaKey: SchemaKey): Either[String, Json] =
    if (supportedSchema.matches(schemaKey))
      config.asRight
    else
      s"Schema key $schemaKey is not supported. A '${supportedSchema.name}' enrichment must have schema '$supportedSchema'.".asLeft
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are
 * configured as PII (personally identifiable information) and apply some anonymization (currently
 * only pseudonymization) on them. Currently a single strategy for all the fields is supported due
 * to the configuration format, and there is only one implemented strategy, however the enrichment
 * supports a strategy per field.
 * The user may specify two types of fields in the config `pojo` or `json`. A `pojo` field is
 * effectively a scalar field in the EnrichedEvent, whereas a `json` is a "context" formatted field
 * and it can either contain a single value in the case of unstruct_event, or an array in the case
 * of derived_events and contexts.
 * @param fieldList list of configured PiiFields
 * @param emitIdentificationEvent to emit an identification event
 * @param strategy pseudonymization strategy to use
 */
final case class PiiPseudonymizerEnrichment(
  fieldList: List[PiiField],
  emitIdentificationEvent: Boolean,
  strategy: PiiStrategy
) extends Enrichment {

  def transformer(event: EnrichedEvent): Option[SelfDescribingData[Json]] = {
    val modifiedFields = fieldList.flatMap(_.transform(event, strategy))
    if (emitIdentificationEvent && modifiedFields.nonEmpty)
      SelfDescribingData(Adapter.UnstructEvent, PiiModifiedFields(modifiedFields, strategy).asJson).some
    else None
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
 * Specifies a strategy to use, a field mutator where the JSON can be found in the EnrichedEvent
 * POJO, a schema criterion to discriminate which contexts to apply this strategy to, and a JSON
 * path within the contexts where this strategy will be applied (the path may correspond to
 * multiple fields).
 * @param fieldMutator the field mutator for the JSON field
 * @param schemaCriterion the schema for which the strategy will be applied
 * @param jsonPath the path where the strategy will be applied
 */
final case class PiiJson(
  fieldMutator: Mutator,
  schemaCriterion: SchemaCriterion,
  jsonPath: String
) extends PiiField {

  override def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields) =
    (for {
      value <- Option(fieldValue)
      parsed <- parse(value).toOption
      (substituted, modifiedFields) = parsed.asObject
        .map { obj =>
          val jObjectMap = obj.toMap
          val contextMapped = jObjectMap.map(mapContextTopFields(_, strategy))
          (
            Json.obj(contextMapped.mapValues(_._1).toList: _*),
            contextMapped.values.flatMap(_._2)
          )
        }
        .getOrElse((parsed, List.empty[JsonModifiedField]))
    } yield (substituted.noSpaces, modifiedFields.toList)).getOrElse((null, List.empty))

  /** Map context top fields with strategy if they match. */
  private def mapContextTopFields(
    tuple: (String, Json),
    strategy: PiiStrategy
  ): (String, (Json, List[JsonModifiedField])) = tuple match {
    case (k, contexts) if k == "data" =>
      (k, contexts.asArray match {
        case Some(array) =>
          val updatedAndModified = array.map(getModifiedContext(_, strategy))
          (
            Json.fromValues(updatedAndModified.map(_._1)),
            updatedAndModified.flatMap(_._2).toList
          )
        case None => getModifiedContext(contexts, strategy)
      })
    case (k, v) => (k, (v, List.empty[JsonModifiedField]))
  }

  /** Returns a modified context or unstruct event along with a list of modified fields. */
  private def getModifiedContext(jv: Json, strategy: PiiStrategy): (Json, List[JsonModifiedField]) =
    jv.asObject
      .map { context =>
        val (obj, fields) = modifyObjectIfSchemaMatches(context.toList, strategy)
        (Json.fromJsonObject(obj), fields)
      }
      .getOrElse((jv, List.empty))

  /**
   * Tests whether the schema for this event matches the schema criterion and if it does modifies
   * it.
   */
  private def modifyObjectIfSchemaMatches(
    context: List[(String, Json)],
    strategy: PiiStrategy
  ): (JsonObject, List[JsonModifiedField]) = {
    val fieldsObj = context.toMap
    (for {
      schema <- fieldsObj.get("schema")
      schemaStr <- schema.asString
      parsedSchemaMatches <- SchemaKey.fromUri(schemaStr).map(schemaCriterion.matches).toOption
      // withFilter generates an unused variable
      _ <- if (parsedSchemaMatches) Some(()) else None
      data <- fieldsObj.get("data")
      updated = jsonPathReplace(data, strategy, schemaStr)
    } yield (
      JsonObject(fieldsObj.updated("schema", schema).updated("data", updated._1).toList: _*),
      updated._2
    )).getOrElse((JsonObject(context: _*), List()))
  }

  /**
   * Replaces a value in the given context with the result of applying the strategy to that value.
   */
  private def jsonPathReplace(
    json: Json,
    strategy: PiiStrategy,
    schema: String
  ): (Json, List[JsonModifiedField]) = {
    val objectNode = io.circe.jackson.mapper.valueToTree[ObjectNode](json)
    val documentContext = JJsonPath.using(JsonPathConf).parse(objectNode)
    val modifiedFields = MutableList[JsonModifiedField]()
    val documentContext2 = documentContext.map(
      jsonPath,
      new ScrambleMapFunction(strategy, modifiedFields, fieldMutator.fieldName, jsonPath, schema)
    )
    // make sure it is a structure preserving method, see #3636
    //val transformedJValue = JsonMethods.fromJsonNode(documentContext.json[JsonNode]())
    //val Diff(_, erroneouslyAdded, _) = jValue diff transformedJValue
    //val Diff(_, withoutCruft, _) = erroneouslyAdded diff transformedJValue
    (jacksonToCirce(documentContext2.json[JsonNode]()), modifiedFields.toList)
  }
}

private final case class ScrambleMapFunction(
  strategy: PiiStrategy,
  modifiedFields: MutableList[JsonModifiedField],
  fieldName: String,
  jsonPath: String,
  schema: String
) extends MapFunction {
  override def map(currentValue: AnyRef, configuration: Configuration): AnyRef =
    currentValue match {
      case s: String =>
        val newValue = strategy.scramble(s)
        val _ = modifiedFields += JsonModifiedField(fieldName, s, newValue, jsonPath, schema)
        newValue
      case a: ArrayNode =>
        a.elements.asScala.map {
          case t: TextNode =>
            val originalValue = t.asText()
            val newValue = strategy.scramble(originalValue)
            modifiedFields += JsonModifiedField(
              fieldName,
              originalValue,
              newValue,
              jsonPath,
              schema
            )
            newValue
          case default: AnyRef => default
        }
      case default: AnyRef => default
    }
}
