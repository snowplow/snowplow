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
package com.snowplowanalytics.snowplow.enrich.hadoop
package utils

// Java
import java.lang.{Byte => JByte}

// Scalaz
import scalaz._
import Scalaz._

// Argonaut
import argonaut._
import Argonaut._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers
import org.scalacheck._
import org.scalacheck.Arbitrary._

class JsonAsStringTest extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the asString extraction function for JSON Strings"                     ^
                                                                                                         p^
  "extracting correctly typed String values from JSONs as String should work"                             ! e1^
  "extracting incorrectly typed values from JSONs as Strings should fail"                                 ! e2^
                                                                                                          end

  val FieldName = "val"
  def err: (String) => String = json => "JSON field [%s]: [%s] is not extractable as a String".format(FieldName, json)

  def e1 =
    "SPEC NAME"           || "JSON"                                | "EXPECTED"                           |
    "JSON String, short"  !! "cancelled".asJson                    ! "cancelled"                          |
    "JSON String, long"   !! "this is a long sentence no?".asJson  ! "this is a long sentence no?"        |
    "JSON String, empty"  !! "".asJson                             ! null                                 |> {
      (_, json, expected) =>
        JsonUtils.asString(FieldName, json) must beSuccessful(expected)
    }

  def e2 =
    "SPEC NAME"        || "JSON"                                   | "EXPECTED"                           |
    "JSON Number"      !! 2304.asJson                              ! err("2304")                          |
    "JSON Number"      !! 11.8302.asJson                           ! err("11.8302")                       |
    "JSON Boolean"     !! true.asJson                              ! err("true")                          |
    "JSON Boolean"     !! false.asJson                             ! err("false")                         |
    "JSON Object"      !! Json("key1" := 3, "key2" := "hello")     ! err("""{"key1":3,"key2":"hello"}""") |> {
      (_, json, expected) =>
        JsonUtils.asString(FieldName, json) must beFailing(expected)
    }
}

class JsonAsJByteTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the asJByte extraction function for JSON Booleans"                     ^
                                                                                                         p^
  "extracting correctly typed Boolean values from JSONs as JBytes should work"                            ! e1^
  "extracting incorrectly typed values from JSONs as JBytes should fail"                                  ! e2^
                                                                                                          end

  val FieldName = "val"
  def err: (String) => String = json => "JSON field [%s]: [%s] is not extractable as a JByte".format(FieldName, json)

  def e1 =
    "SPEC NAME"           || "JSON"                               | "EXPECTED"                            |
    "JSON Boolean, true"  !! true.asJson                          ! 1                                     |
    "JSON Boolean, false" !! false.asJson                         ! 0                                     |> {
      (_, json, expected) =>
        JsonUtils.asJByte(FieldName, json) must beSuccessful(expected)
    }

  def e2 =
    "SPEC NAME"           || "JSON"                               | "EXPECTED"                            |
    "JSON Number"         !! 2304.asJson                          ! err("2304")                           |
    "JSON Number"         !! 11.8302.asJson                       ! err("11.8302")                        |
    "JSON String"         !! "this is a string".asJson            ! err("\"this is a string\"")           | // Note the quotes
    "JSON String"         !! "John Smith".asJson                  ! err("\"John Smith\"")                 | // Note the quotes
    "JSON Object"         !! Json("key1" := 3, "key2" := "hello") ! err("""{"key1":3,"key2":"hello"}""")  |> {
      (_, json, expected) =>
        JsonUtils.asJByte(FieldName, json) must beFailing(expected)
    }
}

class JsonAsJDoubleTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the asJDouble extraction function for JSON Numbers"                     ^
                                                                                                         p^
  "extracting correctly typed Number values from JSONs as JDoubles should work"                            ! e1^
  "extracting incorrectly typed values from JSONs as JDoubles should fail"                                 ! e2^
                                                                                                          end

  val FieldName = "val"
  def err: (String) => String = json => "JSON field [%s]: [%s] is not extractable as a JDouble".format(FieldName, json)

  def e1 =
    "SPEC NAME"                         || "JSON"                            | "EXPECTED"                 |
    "JSON Number, integer"              !! 2321.asJson                       ! 2321.0                     |
    "JSON Number, negative integer"     !! -2129311900.asJson                ! -2129311900                |
    "JSON Number, floating point #1"    !! 69053.0431.asJson                 ! 69053.0431                 |
    "JSON Number, floating point #2"    !! 0.29327442.asJson                 ! 0.29327442                 |> {
      (_, json, expected) =>
        JsonUtils.asJDouble(FieldName, json) must beSuccessful(expected)
    }

  def e2 =
    "SPEC NAME"           || "JSON"                               | "EXPECTED"                            |
    "JSON Boolean"        !! true.asJson                          ! err("true")                           |
    "JSON Boolean"        !! false.asJson                         ! err("false")                          |
    "JSON String"         !! "this is a string".asJson            ! err("\"this is a string\"")           | // Note the quotes
    "JSON String"         !! "John Smith".asJson                  ! err("\"John Smith\"")                 | // Note the quotes
    "JSON Object"         !! Json("key1" := 3, "key2" := "hello") ! err("""{"key1":3,"key2":"hello"}""")  |> {
      (_, json, expected) =>
        JsonUtils.asJDouble(FieldName, json) must beFailing(expected)
    }
}

class JsonAsJIntegerTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the asJInteger extraction function for JSON Numbers"                   ^
                                                                                                         p^
  "extracting correctly typed Number values from JSONs as JIntegers should work"                          ! e1^
  "extracting incorrectly typed values from JSONs as JIntegers should fail"                               ! e2^
                                                                                                          end

  val FieldName = "val"
  def err: (String) => String = json => "JSON field [%s]: [%s] is not extractable as a JInteger".format(FieldName, json)

  def e1 =
    "SPEC NAME"                           || "JSON"                            | "EXPECTED"               |
    "JSON Number, integer"                !! 2321.asJson                       ! 2321                     |
    "JSON Number, negative integer"       !! -2129311900.asJson                ! -2129311900              |
    "JSON Number, rounded floating point" !! 69053.0431.asJson                 ! 69053                    |
    "JSON Number, rounded floating point" !! 0.51327442.asJson                 ! 0                        |> {
      (_, json, expected) =>
        JsonUtils.asJInteger(FieldName, json) must beSuccessful(expected)
    }

  def e2 =
    "SPEC NAME"           || "JSON"                               | "EXPECTED"                            |
    "JSON Boolean"        !! true.asJson                          ! err("true")                           |
    "JSON Boolean"        !! false.asJson                         ! err("false")                          |
    "JSON String"         !! "this is a string".asJson            ! err("\"this is a string\"")           | // Note the quotes
    "JSON String"         !! "John Smith".asJson                  ! err("\"John Smith\"")                 | // Note the quotes
    "JSON Object"         !! Json("key1" := 3, "key2" := "hello") ! err("""{"key1":3,"key2":"hello"}""")  |> {
      (_, json, expected) =>
        JsonUtils.asJInteger(FieldName, json) must beFailing(expected)
    }
}
