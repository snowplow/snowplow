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
package apirequest

import cats.Eval
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mock.Mockito

class HttpApiSpec extends Specification with ValidatedMatchers with Mockito {
  def is = s2"""
  fail to build request string without all keys $e1
  build request string from template context    $e2
  failure on failed HTTP connection             $e3
  """

  def e1 = {
    val httpApi =
      HttpApi("GET", "http://api.acme.com/{{user}}/{{ time}}/", anyInt, Authentication(None))
    val templateContext = Map("user" -> "admin")
    val request = httpApi.buildUrl(templateContext)
    request must beNone
  }

  def e2 = {
    val httpApi =
      HttpApi(
        anyString,
        "http://thishostdoesntexist31337:8123/{{  user }}/foo/{{ time}}/{{user}}",
        anyInt,
        Authentication(None)
      )

    val templateContext = Map("user" -> "admin", "time" -> "November 2015")
    val request = httpApi.buildUrl(templateContext)
    request must beSome("http://thishostdoesntexist31337:8123/admin/foo/November+2015/admin")
  }

  // This one uses real actor system
  def e3 = {
    val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))
    val enrichment = ApiRequestConf(
      schemaKey,
      Nil,
      HttpApi("GET", "http://thishostdoesntexist31337:8123/endpoint", 1000, Authentication(None)),
      List(Output("", Some(JsonOutput("")))),
      Cache(1, 1)
    ).enrichment[Eval]

    val event = new outputs.EnrichedEvent
    val request = enrichment.value.lookup(event, Nil, Nil, None).value
    request must beInvalid
  }
}
