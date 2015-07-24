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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package registry

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import outputs.EnrichedEvent

// Specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Tests the anonymzeIp function
 */
class JavascriptScriptEnrichmentSpec extends Specification with ValidationMatchers { def is =

  "This is a specification to test the JavascriptScriptEnrichment"     ^
                                                                      p^
    "Compiling an invalid JavaScript script should fail"               ! e1^
    "A JavaScript script should be able to throw an exception safely"  ! e2^
    "A JavaScript script should successfully generate a new context"   ! e3^
                                                                       end

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
    JavascriptScriptEnrichment(compiled.toOption.get)
  }

  def buildEvent(appId: String): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.platform = "server"
    e.app_id = appId
    e
  }

  def e1 = {
    val actual = JavascriptScriptEnrichment.compile("[")
    val expectedOracleJdk = Failure("Error compiling JavaScript script: [javax.script.ScriptException: sun.org.mozilla.javascript.internal.EvaluatorException: syntax error (<Unknown Source>#5)]")
    val expectedOpenJdk = Failure("Error compiling JavaScript script: [javax.script.ScriptException: sun.org.mozilla.javascript.EvaluatorException: syntax error (<Unknown Source>#5)]")

    List(expectedOracleJdk, expectedOpenJdk) must contain(actual)
  }

  def e2 = {
    val event = buildEvent("guess")

    val actual = PreparedEnrichment.process(event)
    val expected = "Evaluating JavaScript script threw an exception: [javax.script.ScriptException: Server-side event has invalid app_id: guess in <Unknown Source> at line number 7]"

    actual must beFailing(expected)
  }

  def e3 = {
    val event = buildEvent("secret")

    val actual = PreparedEnrichment.process(event)
    val expected = """{"schema":"iglu:com.acme/foo/jsonschema/1-0-0","data":{"appIdUpper":"SECRET"}}"""

    actual must beSuccessful.like { case head :: Nil => compact(render(head)) must_== compact(render(parse(expected))) }
  }

}
