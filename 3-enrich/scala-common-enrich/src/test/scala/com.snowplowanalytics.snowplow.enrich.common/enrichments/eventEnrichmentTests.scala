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

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// Scalaz
import scalaz._
import Scalaz._

class ExtractEventTypeTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the extractEventType function"                  ^
                                                                                  p^
  "extractEventType should return the event name for any valid event code"         ! e1^
  "extractEventType should return a validation failure for any invalid event code" ! e2^
                                                                                   end

  val FieldName = "e"
  def err: (String) => String = input => "Field [%s]: [%s] is not a recognised event code".format(FieldName, input)

  def e1 =
    "SPEC NAME"                 || "INPUT VAL" | "EXPECTED OUTPUT"  |
    "transaction"               !! "tr"        ! "transaction"      |
    "transaction item"          !! "ti"        ! "transaction_item" |
    "page view"                 !! "pv"        ! "page_view"        |
    "page ping"                 !! "pp"        ! "page_ping"        |
    "unstructured event"        !! "ue"        ! "unstruct"         |
    "structured event"          !! "se"        ! "struct"           |
    "structured event (legacy)" !! "ev"        ! "struct"           |
    "ad impression (legacy)"    !! "ad"        ! "ad_impression"    |> {

      (_, input, expected) => EventEnrichments.extractEventType(FieldName, input) must beSuccessful(expected)
    }

  def e2 =
    "SPEC NAME"       || "INPUT VAL" | "EXPECTED OUTPUT" |
    "null"            !! null        ! err("null")       |
    "empty string"    !! ""          ! err("")           |
    "unrecognized #1" !! "e"         ! err("e")          |
    "unrecognized #2" !! "evnt"      ! err("evnt")       |> {

      (_, input, expected) => EventEnrichments.extractEventType(FieldName, input) must beFailing(expected)
    }

}
