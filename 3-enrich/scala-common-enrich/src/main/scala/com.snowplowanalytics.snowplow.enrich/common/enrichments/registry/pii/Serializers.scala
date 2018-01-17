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
package pii

// Json4s
import org.json4s.JsonDSL._
import org.json4s.Extraction.decompose
import org.json4s.{CustomSerializer, JObject}

// Scalaz
import scalaz.{Failure, Success}

/**
 * Custom serializer for PiiStrategy class
 */
private[pii] final class PiiStrategySerializer
    extends CustomSerializer[PiiStrategy](formats =>
      ({
        case jo: JObject =>
          implicit val json4sFormats = formats
          val function               = (jo \ "pseudonymize" \ "hashFunction").extract[String]
          PiiPseudonymizerEnrichment.getHashFunction(function) match {
            case Success(hf) => PiiStrategyPseudonymize(function, hf)
            case Failure(msg) =>
              println(msg); PiiStrategyPseudonymize("IDENTITY", (b: Array[Byte]) => b.mkString) // FIXME: What to do here?
          }
      }, {
        case psp: PiiStrategyPseudonymize =>
          "pseudonymize" -> ("hashFunction" -> psp.functionName)
      }))

/**
 * Custom serializer for PiiModifiedFields class
 */
private[pii] final class PiiModifiedFieldsSerializer
    extends CustomSerializer[PiiModifiedFields](formats => {
      val PiiTransformationSchema = "iglu:com.snowplowanalytics.snowplow/pii_transformation/jsonschema/1-0-0"
      ({
        case jo: JObject =>
          implicit val json4sFormats = formats
          val fields                 = (jo \ "data" \ "pii").extract[List[ModifiedField]]
          val strategy               = (jo \ "data" \ "strategy").extract[PiiStrategy]
          PiiModifiedFields(fields, strategy)
      }, {
        case pmf: PiiModifiedFields =>
          implicit val json4sFormats = formats
          ("schema" -> PiiTransformationSchema) ~
            ("data" ->
              ("pii" -> decompose(
                pmf.modifiedFields.foldLeft(Map.empty[String, List[ModifiedField]]) {
                  case (m, mf) =>
                    mf match {
                      case s: ScalarModifiedField =>
                        m + ("pojo" -> (s :: m.getOrElse("pojo", List.empty[ModifiedField])))
                      case j: JsonModifiedField => m + ("json" -> (j :: m.getOrElse("json", List.empty[ModifiedField])))
                    }
                }
              )) ~
                ("strategy" -> decompose(pmf.strategy)))
      })
    })
