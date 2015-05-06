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
package hadoop

// Iglu Scala Client
import iglu.client.Resolver

// This project
import utils.JsonUtils

/**
 * Holds general helpers for running the
 * Specs2 tests.
 */
object SpecHelpers {

  // Internal
  private val igluConfigField = "IgluConfigField"

  /**
   * Standard Iglu configuration.
   */
  val IgluConfig =
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
    json <- JsonUtils.extractJson(igluConfigField, IgluConfig)
    reso <- Resolver.parse(json)
  } yield reso).getOrElse(throw new RuntimeException("Could not build an Iglu resolver, should never happen")) 

}
