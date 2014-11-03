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
package adapters
package registry

// Iglu
import iglu.client.Resolver

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Snowplow
import loaders.CollectorPayload
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class AdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the Adapter trait's functionality"                                                  ^
                                                                                                                      p^
  "toMap should convert a list of name-value pairs into a map"                                                         ! e1^
  "toUnstructEventParams should generate a boilerplate set of parameters for an empty unstructured event"              ! e2^
  "toUnstructEventParams should preserve nuid, aid, cv and p outside of the unstructured event"                        ! e3^
                                                                                                                       end
  // TODO: add test for buildFormatter()

  implicit val resolver = SpecHelpers.IgluResolver

  object BaseAdapter extends Adapter {
    def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver) = "Base".failNel
  }

  def e1 = {
    val pairs = toNameValuePairs("a" -> "1", "b" -> "2", "c" -> "3")
    BaseAdapter.toMap(pairs) must_== Map("a" -> "1", "b" -> "2", "c" -> "3")
  }

  def e2 = {
    val params = BaseAdapter.toUnstructEventParams("tv", Map[String, String](), "iglu:foo", _ => List[JField]())
    params must_== Map(
      "tv"    -> "tv",
      "e"     -> "ue",
      "p"     -> "app",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }

  def e3 = {
    val shared = Map("nuid" -> "123", "aid" -> "42", "cv" -> "clj-tomcat", "p" -> "srv")
    val params = BaseAdapter.toUnstructEventParams("tv", shared, "iglu:foo", _ => List[JField]())
    params must_== shared ++ Map(
      "tv"    -> "tv",
      "e"     -> "ue",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }
}
