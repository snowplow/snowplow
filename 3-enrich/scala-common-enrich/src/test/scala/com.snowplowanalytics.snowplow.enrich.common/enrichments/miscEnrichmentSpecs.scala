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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import io.circe.literal._
import org.specs2.mutable.{Specification => MutSpecification}
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DataTables
import scalaz._
import Scalaz._

class EtlVersionSpec extends MutSpecification {
  "The ETL version" should {
    "be successfully returned using an x.y.z format" in {
      val anyString = "spark-x.x.x"
      MiscEnrichments.etlVersion(anyString) must beMatching(
        s"${anyString}-common-\\d+\\.\\d+\\.\\d+(-\\w+)?".r)
    }
  }
}

/** Tests the extractPlatform function. Uses DataTables. */
class ExtractPlatformSpec extends Specification with DataTables {
  val FieldName = "p"
  def err: (String) => String =
    input => "Field [%s]: [%s] is not a supported tracking platform".format(FieldName, input)

  def is = s2"Extracting platforms with extractPlatform should work $e1"

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "valid web" !! "web" ! "web".success |
      "valid mobile/tablet" !! "mob" ! "mob".success |
      "valid desktop/laptop/netbook" !! "pc" ! "pc".success |
      "valid server-side app" !! "srv" ! "srv".success |
      "valid general app" !! "app" ! "app".success |
      "valid connected TV" !! "tv" ! "tv".success |
      "valid games console" !! "cnsl" ! "cnsl".success |
      "valid iot (internet of things)" !! "iot" ! "iot".success |
      "invalid empty" !! "" ! err("").fail |
      "invalid null" !! null ! err(null).fail |
      "invalid platform" !! "ma" ! err("ma").fail |> { (_, input, expected) =>
      MiscEnrichments.extractPlatform(FieldName, input) must_== expected
    }
}

class ExtractIpSpec extends Specification with DataTables {

  def is = s2"Extracting ips with extractIp should work $e1"

  val nullString: String = null

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "single ip" !! "127.0.0.1" ! "127.0.0.1".success |
      "ips ', '-separated" !! "127.0.0.1, 127.0.0.2" ! "127.0.0.1".success |
      "ips ','-separated" !! "127.0.0.1,127.0.0.2" ! "127.0.0.1".success |
      "ips separated out of the spec" !! "1.0.0.1!1.0.0.2" ! "1.0.0.1!1.0.0.2".success |
      // ConversionUtils.makeTsvSafe returns null for empty string
      "empty" !! "" ! Success(null) |
      "null" !! null ! Success(null) |> { (_, input, expected) =>
      MiscEnrichments.extractIp("ip", input) must_== expected
    }

}

/**
 * Tests the identity function.
 * Uses ScalaCheck.
 */
class IdentitySpec extends Specification with ScalaCheck {

  def is =
    "The identity function should work for any pair of Strings" ! e1

  def e1 =
    check { (field: String, value: String) =>
      MiscEnrichments.identity(field, value) must_== value.success
    }
}

class FormatDerivedContextsSpec extends MutSpecification {

  "extractDerivedContexts" should {
    "convert a list of JObjects to a self-describing contexts JSON" in {

      val derivedContextsList = List(
        json"""
          {
            "schema": "iglu:com.acme/user/jsonschema/1-0-0",
            "data": {
              "type": "tester",
              "name": "bethany"
            }
          }
        """,
        json"""
          {
            "schema": "iglu:com.acme/design/jsonschema/1-0-0",
            "data": {
              "color": "red",
              "fontSize": 14
            }
          }
        """
      )

      val expected = """
      |{
        |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
        |"data":[
        |{
          |"schema":"iglu:com.acme/user/jsonschema/1-0-0",
          |"data":{
            |"type":"tester",
            |"name":"bethany"
          |}
        |},
        |{
          |"schema":"iglu:com.acme/design/jsonschema/1-0-0",
          |"data":{
            |"color":"red",
            |"fontSize":14
            |}
          |}
        |]
      |}""".stripMargin.replaceAll("[\n\r]", "")

      MiscEnrichments.formatDerivedContexts(derivedContextsList) must_== expected
    }
  }
}
