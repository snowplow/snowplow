/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments.registry.sqlquery

import cats.implicits._

import io.circe.{Decoder, DecodingFailure, Json}
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.SelfDescribingData

/**
 * Container class for output preferences.
 * Describes how to transform data fetched from DB into derived contexts
 * @param json JSON-preferences
 * @param expectedRows specifies amount of expected rows
 */
final case class Output(json: JsonOutput, expectedRows: Output.ExpectedRowsMode) {
  import Output._

  /**
   * Validate output according to expectedRows and describe
   * (attach Schema URI) to context according to json.describes.
   * @param jsons list of JSON Objects derived from SQL rows (row is always JSON Object)
   * @return validated list of described JSONs
   */
  def envelope(jsons: List[Json]): Either[Throwable, List[SelfDescribingData[Json]]] =
    (json.describes, expectedRows) match {
      case (DescribeMode.AllRows, AtLeastOne) =>
        AtLeastOne
          .collect(jsons)
          .map { jobjs =>
            List(describe(Json.arr(jobjs: _*)))
          }
      case (DescribeMode.AllRows, AtLeastZero) =>
        AtLeastZero
          .collect(jsons)
          .map { jobjs =>
            List(describe(Json.arr(jobjs: _*)))
          }
      case (DescribeMode.AllRows, single) =>
        single.collect(jsons).map(_.headOption.map(describe).toList)
      case (DescribeMode.EveryRow, any) =>
        any.collect(jsons).map(_.map(describe))
    }

  private def describe(data: Json): SelfDescribingData[Json] =
    SelfDescribingData(json.schema, data)
}

object Output {

  /**
   * ADT specifying whether the schema is the self-describing schema for all
   * rows returned by the query, or whether the schema should be attached to
   * each of the returned rows.
   * Processing in [[Output#envelope]]
   */
  sealed trait DescribeMode

  object DescribeMode {

    /**
     * Box all returned rows - i.e. one context will always be added to
     * derived_contexts, regardless of how many rows that schema contains
     * Can be List(JArray) (signle) or List(JObject) (signle) or Nil
     */
    case object AllRows extends DescribeMode

    /**
     * Attached Schema URI to each returned row - so e.g. if 3 rows are returned,
     * 3 contexts with this same schema will be added to derived_contexts
     * Can be List(JObject, JObject...) (multiple) | Nil
     */
    case object EveryRow extends DescribeMode

  }

  /**
   * ADT specifying what amount of rows are expecting from DB
   */
  sealed trait ExpectedRowsMode {

    /**
     * Validate some amount of rows against predefined expectation
     * @param resultSet JSON objects fetched from DB
     * @return same list of JSON object as right disjunction if amount
     *         of rows matches expectation or [[InvalidDbResponse]] as
     *         left disjunction if amount is lower or higher than expected
     */
    def collect(resultSet: List[Json]): Either[Throwable, List[Json]]
  }

  /**
   * Exactly one row is expected. 0 or 2+ rows will throw an error, causing the entire event to fail
   * processing
   */
  case object ExactlyOne extends ExpectedRowsMode {
    def collect(resultSet: List[Json]): Either[Throwable, List[Json]] =
      resultSet match {
        case List(one) => List(one).asRight
        case _ =>
          InvalidDbResponse(s"SQL Query Enrichment: exactly one row was expected").asLeft
      }
  }

  /** Either one or zero rows is expected. 2+ rows will throw an error */
  case object AtMostOne extends ExpectedRowsMode {
    def collect(resultSet: List[Json]): Either[Throwable, List[Json]] =
      resultSet match {
        case List(one) => List(one).asRight
        case Nil => Nil.asRight
        case _ =>
          InvalidDbResponse(s"SQL Query Enrichment: at most one row was expected").asLeft
      }
  }

  /** Always successful */
  case object AtLeastZero extends ExpectedRowsMode {
    def collect(resultSet: List[Json]): Either[Throwable, List[Json]] =
      resultSet.asRight
  }

  /** More that 1 rows are expected 0 rows will throw an error */
  case object AtLeastOne extends ExpectedRowsMode {
    def collect(resultSet: List[Json]): Either[Throwable, List[Json]] =
      resultSet match {
        case Nil =>
          InvalidDbResponse(s"SQL Query Enrichment: at least one row was expected. 0 given instead").asLeft
        case other => other.asRight
      }
  }

  implicit val outputCirceDecoder: Decoder[Output] =
    deriveDecoder[Output]

  implicit val expectedRowsCirceDecoder: Decoder[ExpectedRowsMode] =
    Decoder.instance { cur =>
      cur.as[String].flatMap { str =>
        str match {
          case "EXACTLY_ONE" => ExactlyOne.asRight
          case "AT_MOST_ONE" => AtMostOne.asRight
          case "AT_LEAST_ONE" => AtLeastOne.asRight
          case "AT_LEAST_ZERO" => AtLeastZero.asRight
          case _ => DecodingFailure(s"$str is not valid ExpectedRowMode", cur.history).asLeft
        }
      }
    }

  implicit val describeModeCirceDecoder: Decoder[DescribeMode] =
    Decoder.instance { cur =>
      cur.as[String].flatMap { str =>
        str match {
          case "ALL_ROWS" => DescribeMode.AllRows.asRight
          case "EVERY_ROW" => DescribeMode.EveryRow.asRight
          case _ => DecodingFailure(s"$str is not valid DescribeMode", cur.history).asLeft
        }
      }
    }
}
