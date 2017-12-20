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

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// Joda
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

// Scalaz
import scalaz._
import Scalaz._

class ExtractEventTypeSpec extends Specification with DataTables with ValidationMatchers { def is = s2"""
  This is a specification to test the extractEventType function
  extractEventType should return the event name for any valid event code         $e1
  extractEventType should return a validation failure for any invalid event code $e2
  formatCollectorTstamp should validate collector timestamps                     $e3
  extractTimestamp should validate timestamps                                    $e4
  """

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

  val SeventiesTstamp = Some(new DateTime(0, DateTimeZone.UTC))
  val BCTstamp = SeventiesTstamp.map(_.minusYears(2000))
  val FarAwayTstamp = SeventiesTstamp.map(_.plusYears(10000))

  def e3 =
    "SPEC NAME"          || "INPUT VAL"     | "EXPECTED OUTPUT"                                                                                     |
    "None"               !! None            ! "No collector_tstamp set".fail                                                                        |
    "Negative timestamp" !! BCTstamp        ! "Collector timestamp [-63113904000000] formatted as [-0030-01-01 00:00:00.000] which isn't Redshift-compatible".fail |
    ">10k timestamp"     !! FarAwayTstamp   ! "Collector timestamp [315569520000000] formatted as [11970-01-01 00:00:00.000] which isn't Redshift-compatible".fail |
    "Valid timestamp"    !! SeventiesTstamp ! "1970-01-01 00:00:00.000".success                                                                     |> {

      (_, input, expected) => EventEnrichments.formatCollectorTstamp(input) must_== (expected)
    }

  def e4 =
    "SPEC NAME" || "INPUT VAL"               | "EXPECTED OUTPUT"                                                                                               |
    "Not long"  !! ("f", "v")                ! "Field [f]: [v] is not in the expected format (ms since epoch)".fail                                            |
    "Too long"  !! ("f", "1111111111111111") ! "Field [f]: [1111111111111111] is formatted as [37179-09-17 07:18:31.111] which isn't Redshift-compatible".fail |
    "Valid ts"  !! ("f", "1")                ! "1970-01-01 00:00:00.001".success                                                                               |> {
      (_, input, expected) => EventEnrichments.extractTimestamp(input._1, input._2) must_== (expected)
    }
}

class DerivedTimestampSpec extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the getDerivedTimestamp function"                  ^
                                                                                     p^
  "getDerivedTimestamp should correctly calculate the derived timestamp "             ! e1^
                                                                                      end
  def e1 =
    "SPEC NAME"                                   || "DVCE_CREATED_TSTAMP"     | "DVCE_SENT_TSTAMP"        | "COLLECTOR_TSTAMP"        | "TRUE_TSTAMP"             | "EXPECTED DERIVED_TSTAMP" |
    "No dvce_sent_tstamp"                         !! "2014-04-29 12:00:54.555" ! null                      ! "2014-04-29 09:00:54.000" ! null                      ! "2014-04-29 09:00:54.000" |
    "No dvce_created_tstamp"                      !! null                      ! null                      ! "2014-04-29 09:00:54.000" ! null                      ! "2014-04-29 09:00:54.000" |
    "No collector_tstamp"                         !! null                      ! null                      ! null                      ! null                      ! null                      |
    "dvce_sent_tstamp before dvce_created_tstamp" !! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.000" ! null                      ! "2014-04-29 09:00:54.000" |
    "dvce_sent_tstamp after dvce_created_tstamp"  !! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! null                      ! "2014-04-29 09:00:53.999" |
    "true_tstamp override"                        !! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.000" ! "2000-01-01 00:00:00.000" ! "2000-01-01 00:00:00.000" |> {

      (_, created, sent, collected, truth, expected) =>
        EventEnrichments.getDerivedTimestamp(Option(sent), Option(created), Option(collected), Option(truth)) must beSuccessful(Option(expected))
    }
}
