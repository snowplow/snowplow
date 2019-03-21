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
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.syntax.either._
import io.circe._
import io.gatling.jsonpath.{JsonPath => GatlingJsonPath}

/** Wrapper for `io.gatling.jsonpath` for circe and scalaz */
object JsonPath {

  /**
   * Wrapper method for not throwing an exception on JNothing, representing it as invalid JSON
   * @param json JSON value, possibly JNothing
   * @return successful POJO on any JSON except JNothing
   */
  def convertToJson(json: Json): Object =
    io.circe.jackson.mapper.convertValue(json, classOf[Object])

  /**
   * Pimp-up JsonPath class to work with JValue
   * Unlike `query(jsonPath, json)` it gives empty list on any error (like JNothing)
   * @param jsonPath precompiled with [[compileQuery]] JsonPath object
   */
  implicit class CirceExtractor(jsonPath: GatlingJsonPath) {
    def circeQuery(json: Json): List[Json] = {
      val pojo = convertToJson(json)
      jsonPath.query(pojo).map(anyToJson).toList
    }
  }

  /**
   * Query some JSON by `jsonPath`. It always return List, even for single match.
   * Unlike `json.circeQuery(stringPath)` it gives error if JNothing was given
   */
  def query(jsonPath: String, json: Json): Either[String, List[Json]] = {
    val pojo = convertToJson(json)
    GatlingJsonPath.query(jsonPath, pojo) match {
      case Right(iterator) => iterator.map(anyToJson).toList.asRight
      case Left(error) => error.reason.asLeft
    }
  }

  /**
   * Precompile JsonPath query
   * @param query JsonPath query as a string
   * @return valid JsonPath object either error message
   */
  def compileQuery(query: String): Either[String, GatlingJsonPath] =
    GatlingJsonPath.compile(query).leftMap(_.reason)

  /**
   * Wrap list of values into JSON array if several values present
   * Use in conjunction with `query`. JNothing will represent absent value
   * @param values list of JSON values
   * @return array if there's >1 values in list
   */
  def wrapArray(values: List[Json]): Json = values match {
    case Nil => Json.fromValues(List.empty)
    case one :: Nil => one
    case many => Json.fromValues(many)
  }

  /**
   * Convert POJO to JValue with `jackson` mapper
   * @param any raw JVM type representing JSON
   * @return Json
   */
  private def anyToJson(any: Any): Json =
    if (any == null) Json.Null
    else io.circe.jackson.mapper.convertValue(any, classOf[Json])
}
