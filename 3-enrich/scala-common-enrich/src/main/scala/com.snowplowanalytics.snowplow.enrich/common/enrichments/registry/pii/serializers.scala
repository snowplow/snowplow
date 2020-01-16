/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import cats.syntax.either._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object serializers {
  implicit val modifiedFieldEncoder: Encoder[ModifiedField] = Encoder.instance {
    case m: ScalarModifiedField => m.asJson
    case m: JsonModifiedField => m.asJson
  }

  implicit val piiStrategyEncoder: Encoder[PiiStrategy] = Encoder.instance {
    case s: PiiStrategyPseudonymize =>
      Json.obj(
        "pseudonymize" := Json.obj(
          "hashFunction" := s.functionName
        )
      )
  }

  implicit val piiModifiedFieldsEncoder: Encoder[PiiModifiedFields] =
    new Encoder[PiiModifiedFields] {
      val PiiTransformationSchema =
        "iglu:com.snowplowanalytics.snowplow/pii_transformation/jsonschema/1-0-0"
      final def apply(a: PiiModifiedFields): Json =
        Json.obj(
          "schema" := PiiTransformationSchema,
          "data" := Json.obj(
            "pii" :=
              a.modifiedFields
                .foldLeft(Map.empty[String, List[ModifiedField]]) {
                  case (m, mf) =>
                    mf match {
                      case s: ScalarModifiedField =>
                        m + ("pojo" -> (s :: m.getOrElse("pojo", List.empty[ModifiedField])))
                      case j: JsonModifiedField =>
                        m + ("json" -> (j :: m.getOrElse("json", List.empty[ModifiedField])))
                    }
                }
                .asJson,
            "strategy" := a.strategy.asJson
          )
        )
    }

  implicit val piiStrategyPseudonymizeDecoder: Decoder[PiiStrategyPseudonymize] =
    new Decoder[PiiStrategyPseudonymize] {
      final def apply(c: HCursor): Decoder.Result[PiiStrategyPseudonymize] =
        for {
          function <- c.downField("pseudonymize").get[String]("hashFunction")
          hashFn <- PiiPseudonymizerEnrichment
            .getHashFunction(function)
            .leftMap(DecodingFailure(_, List.empty))
          salt <- c.downField("pseudonymize").get[String]("salt")
        } yield PiiStrategyPseudonymize(function, hashFn, salt)
    }
}
