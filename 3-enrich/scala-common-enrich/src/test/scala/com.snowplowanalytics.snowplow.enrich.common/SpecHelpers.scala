/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow
package enrich
package common

// Apache URLEncodedUtils
import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

// Iglu Scala Client
import iglu.client.Resolver

// This project
import utils.JsonUtils

// Scalaz
import scalaz._
import Scalaz._

object SpecHelpers {

  // Internal
  private val igluConfigField = "Iglu config field"

  // Standard Iglu configuration
  private val igluConfig =
     """|{
          |"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
          |"data": {
            |"cacheSize": 500,
            |"repositories": [
              |{
                |"name": "Iglu Central",
                |"priority": 0,
                |"vendorPrefixes": [ "com.snowplowanalytics" ],
                |"connection": {
                  |"http": {
                    |"uri": "http://iglucentral.com"
                  |}
                |}
              |}
            |]
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")

  /**
   * Builds an Iglu resolver from
   * the above Iglu configuration.
   */
  val IgluResolver = (for {
    json <- JsonUtils.extractJson(igluConfigField, igluConfig)
    reso <- Resolver.parse(json)
  } yield reso).getOrElse(throw new RuntimeException("Could not build an Iglu resolver, should never happen"))

  private type NvPair = Tuple2[String, String]

  /**
   * Converts an NvPair into a
   * BasicNameValuePair
   *
   * @param pair The Tuple2[String, String] name-value
   * pair to convert
   * @return the basic name value pair 
   */
  private def toNvPair(pair: NvPair): BasicNameValuePair =
    new BasicNameValuePair(pair._1, pair._2)

  /**
   * Converts the supplied NvPairs into a 
   * a NameValueNel.
   *
   * @param head The first NvPair to convert
   * @param tail The rest of the NvPairs to
   * convert
   * @return the populated NvGetPayload
   */
  def toNameValuePairs(pairs: NvPair*): List[NameValuePair] =
    List(pairs.map(toNvPair(_)): _*)

  /**
   * Builds a self-describing JSON by
   * wrapping the supplied JSON with
   * schema and data properties
   *
   * @param json The JSON to use as the request body
   * @param schema The name of the schema to insert
   * @return a self-describing JSON
   */
  def toSelfDescJson(json: String, schema: String): String =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/${schema}/jsonschema/1-0-0","data":${json}}"""
}
