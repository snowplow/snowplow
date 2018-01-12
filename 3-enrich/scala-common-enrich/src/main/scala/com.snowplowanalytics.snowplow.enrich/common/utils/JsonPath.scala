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
package com.snowplowanalytics.snowplow.enrich.common.utils

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._
import org.json4s.jackson.JsonMethods.mapper

// Gatling JsonPath
import io.gatling.jsonpath.{ JsonPath => GatlingJsonPath }

/**
 * Wrapper for `io.gatling.jsonpath` for `json4s` and `scalaz`
 */
object JsonPath {

  private val json4sMapper = mapper

  /**
   * Wrapper method for not throwing an exception on JNothing,
   * representing it as invalid JSON
   *
   * @param json JSON value, possibly JNothing
   * @return successful POJO on any JSON except JNothing
   */
  def convertToJValue(json: JValue): Validation[String, Object] = {
    json match {
      case JNothing => "JSONPath error: Nothing was given".failure
      case other    => json4sMapper.convertValue(other, classOf[Object]).success
    }
  }

  /**
   * Pimp-up JsonPath class to work with JValue
   * Unlike `query(jsonPath, json)` it gives empty list on any error (like JNothing)
   *
   * @param jsonPath precompiled with [[compileQuery]] JsonPath object
   */
  implicit class Json4sExtractor(jsonPath: GatlingJsonPath) {
    def json4sQuery(json: JValue): List[JValue] = {
      convertToJValue(json) match {
        case Success(pojo) => jsonPath.query(pojo).map(anyToJValue).toList
        case Failure(_) => Nil
      }
    }
  }

  /**
   * Query some JSON by `jsonPath`
   * It always return List, even for single match
   * Unlike `jValue.json4sQuery(stringPath)` it gives error if JNothing was given
   */
  def query(jsonPath: String, json: JValue): Validation[String, List[JValue]] = {
    convertToJValue(json).flatMap { pojo =>
      GatlingJsonPath.query(jsonPath, pojo) match {
        case Right(iterator) => iterator.map(anyToJValue).toList.success
        case Left(error)     => error.reason.fail
      }
    }
  }

  /**
   * Precompile JsonPath query
   *
   * @param query JsonPath query as a string
   * @return valid [[JsonPath]] object either error message
   */
  def compileQuery(query: String): Validation[String, GatlingJsonPath] =
    GatlingJsonPath.compile(query)
      .leftMap(_.reason)
      .disjunction
      .validation

  /**
   * Wrap list of values into JSON array if several values present
   * Use in conjunction with `query`. [[JNothing]] will represent absent value
   *
   * @param values list of JSON values
   * @return array if there's >1 values in list
   */
  def wrapArray(values: List[JValue]): JValue = values match {
    case Nil => JNothing
    case one :: Nil => one
    case many => JArray(many)
  }

  /**
   * Convert POJO to JValue with `jackson` mapper
   *
   * @param any raw JVM type representing JSON
   * @return JValue
   */
  private def anyToJValue(any: Any): JValue =
    if (any == null) JNull
    else json4sMapper.convertValue(any, classOf[JValue])

}
