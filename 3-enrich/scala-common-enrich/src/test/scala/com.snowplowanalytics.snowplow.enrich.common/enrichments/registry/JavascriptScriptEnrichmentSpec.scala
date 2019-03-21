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
package enrichments.registry

import io.circe.literal._
import org.specs2.Specification

import outputs.EnrichedEvent

/** Tests the anonymzeIp function */
class JavascriptScriptEnrichmentSpec extends Specification {
  def is = s2"""
  This is a specification to test the JavascriptScriptEnrichment
  Compiling an invalid JavaScript script should fail              $e1
  A JavaScript script should be able to throw an exception safely $e2
  A JavaScript script should successfully generate a new context  $e3
  """

  val PreparedEnrichment = {
    val script =
      s"""|function process(event) {
          |  var platform = event.getPlatform(),
          |      appId    = event.getApp_id();
          |
          |  if (platform == "server" && appId != "secret") {
          |    throw "Server-side event has invalid app_id: " + appId;
          |  }
          |
          |  // Use new String() because http://nelsonwells.net/2012/02/json-stringify-with-mapped-variables/
          |  var appIdUpper = new String(appId.toUpperCase());
          |  return [ { schema: "iglu:com.acme/foo/jsonschema/1-0-0",
          |             data:   { appIdUpper: appIdUpper }
          |           } ];
          |}
          |""".stripMargin

    val compiled = JavascriptScriptEnrichment.compile(script)
    JavascriptScriptEnrichment(compiled.right.get)
  }

  def buildEvent(appId: String): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.platform = "server"
    e.app_id = appId
    e
  }

  def e1 = {
    val actual = JavascriptScriptEnrichment.compile("[")
    actual must beLeft
  }

  def e2 = {
    val event = buildEvent("guess")
    val actual = PreparedEnrichment.process(event)
    actual must beLeft
  }

  def e3 = {
    val event = buildEvent("secret")
    val actual = PreparedEnrichment.process(event)
    val expected =
      json"""{"schema":"iglu:com.acme/foo/jsonschema/1-0-0","data":{"appIdUpper":"SECRET"}}"""
    actual must beRight.like { case head :: Nil => head must_== expected }
  }

}
