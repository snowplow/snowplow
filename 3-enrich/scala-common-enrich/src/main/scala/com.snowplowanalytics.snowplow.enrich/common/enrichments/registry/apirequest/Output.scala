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
package enrichments.registry.apirequest

import cats.syntax.either._
import io.circe._
import io.circe.parser._
import io.circe.syntax._

import utils.JsonPath.{query, wrapArray}

/**
 * Base trait for API output format. Primary intention of these classes is to perform transformation
 * of API raw output to self-describing JSON instance
 */
case class Output(schema: String, json: Option[JsonOutput]) {

  /**
   * Transforming raw API response (text) to JSON (in future A => JSON) and extracting value by
   * output's path
   * @param apiResponse response taken from `ApiMethod`
   * @return parsed extracted JSON
   */
  def parseResponse(apiResponse: String): Either[Throwable, Json] = json match {
    case Some(jsonOutput) => jsonOutput.parseResponse(apiResponse)
    case output =>
      new InvalidStateException(s"Error: Unknown output [$output]").asLeft // Cannot happen now
  }

  /**
   * Extract value specified by output's path
   * @param value parsed API response
   * @return extracted validated JSON
   */
  def extract(value: Json): Either[Throwable, Json] = json match {
    case Some(jsonOutput) => jsonOutput.extract(value)
    case output =>
      new InvalidStateException(s"Error: Unknown output [$output]").asLeft // Cannot happen now
  }

  /**
   * Add `schema` (Iglu URI) to parsed instance
   * @param json JValue parsed from API
   * @return self-describing JSON instance
   */
  def describeJson(json: Json): Json =
    Json.obj(
      "schema" := schema,
      "data" := json
    )
}

/**
 * Common trait for all API output formats
 * @tparam A type of API response (XML, JSON, etc)
 */
sealed trait ApiOutput[A] {
  val path: String

  /**
   * Parse raw response into validated Output format (XML, JSON)
   * @param response API response assumed to be JSON
   * @return validated JSON
   */
  def parseResponse(response: String): Either[Throwable, A]

  /**
   * Extract value specified by `path` and transform to context-ready JSON data
   * @param response parsed API response
   * @return extracted by `path` value mapped to JSON
   */
  def extract(response: A): Either[Throwable, Json]

  /**
   * Try to parse string as JSON and extract value by JSON PAth
   * @param response API response assumed to be JSON
   * @return validated extracted value
   */
  def get(response: String): Either[Throwable, Json] =
    for {
      validated <- parseResponse(response)
      result <- extract(validated)
    } yield result
}

/**
 * Preference for extracting JSON from API output
 * @param jsonPath JSON Path to required value
 */
case class JsonOutput(jsonPath: String) extends ApiOutput[Json] {
  val path = jsonPath

  /**
   * Proxy function for `query` which wrap missing value in error
   * @param json JSON value to look in
   * @return validated found JSON, with absent value treated like failure
   */
  def extract(json: Json): Either[Throwable, Json] =
    query(path, json).map(wrapArray) match {
      case Right(js) if js.asArray.map(_.isEmpty).getOrElse(false) =>
        ValueNotFoundException(
          s"Error: no values were found by JSON Path [$jsonPath] in [${json.noSpaces}]").asLeft
      case other => other.leftMap(JsonPathException.apply)
    }

  def parseResponse(response: String): Either[Throwable, Json] =
    parse(response)
}
