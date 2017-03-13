/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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

// Specs2 & ScalaCheck
import org.specs2.mutable.{Specification => MutSpecification}
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.scalacheck._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
 * Tests the etlVersion variable.
 * Uses mutable.Specification.
 */
class EtlVersionSpec extends MutSpecification {

  "The ETL version" should {
    "be successfully returned" in {
      MiscEnrichments.etlVersion("hadoop-x.x.x") must_== "hadoop-x.x.x-common-0.24.1-M1"
    }
  }
}

/**
 * Tests the extractPlatform function.
 * Uses DataTables.
 */
class ExtractPlatformSpec extends Specification with DataTables {

  val FieldName = "p"
  def err: (String) => String = input => "Field [%s]: [%s] is not a supported tracking platform".format(FieldName, input)

  def is =
    "Extracting platforms with extractPlatform should work" ! e1

  def e1 =
    "SPEC NAME"                      || "INPUT VAL" | "EXPECTED OUTPUT" |
    "valid web"                      !! "web"       ! "web".success     |
    "valid mobile/tablet"            !! "mob"       ! "mob".success     |
    "valid desktop/laptop/netbook"   !! "pc"        ! "pc".success      |
    "valid server-side app"          !! "srv"       ! "srv".success     |
    "valid general app"              !! "app"       ! "app".success     |
    "valid connected TV"             !! "tv"        ! "tv".success      |
    "valid games console"            !! "cnsl"      ! "cnsl".success    |
    "valid iot (internet of things)" !! "iot"       ! "iot".success     |
    "invalid empty"                  !! ""          !  err("").fail     |
    "invalid null"                   !! null        !  err(null).fail   |
    "invalid platform"               !! "ma"        !  err("ma").fail   |> {

      (_, input, expected) => MiscEnrichments.extractPlatform(FieldName, input) must_== expected
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
    check { (field: String, value: String) => MiscEnrichments.identity(field, value) must_== value.success }
}

class FormatDerivedContextsSpec extends MutSpecification {

  "extractDerivedContexts" should {
    "convert a list of JObjects to a self-describing contexts JSON" in {

      val derivedContextsList = List(
          ( ("schema" -> "iglu:com.acme/user/jsonschema/1-0-0") ~
            ("data" ->
              ("type" -> "tester") ~
              ("name" -> "bethany")
            )
          ),
          ( ("schema" -> "iglu:com.acme/design/jsonschema/1-0-0") ~
            ("data" ->
              ("color" -> "red") ~
              ("fontSize" -> 14)
            )
          )
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
      |}""".stripMargin.replaceAll("[\n\r]","")

      MiscEnrichments.formatDerivedContexts(derivedContextsList) must_== expected
    }
  }
}
