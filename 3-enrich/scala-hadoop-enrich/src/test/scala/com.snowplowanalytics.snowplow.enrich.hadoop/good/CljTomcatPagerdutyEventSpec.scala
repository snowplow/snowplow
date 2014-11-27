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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package good

// Scala
import scala.collection.mutable.Buffer

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatPagerdutyEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.pagerduty/v1   404 -  -    aid=crons&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fjson   eyJtZXNzYWdlcyI6W3sidHlwZSI6ImluY2lkZW50LnRyaWdnZXIiLCJkYXRhIjp7ImluY2lkZW50Ijp7ImlkIjoiUDlXWTlVOSIsImluY2lkZW50X251bWJlciI6MTM5LCJjcmVhdGVkX29uIjoiMjAxNC0xMS0xMlQxODo1Mzo0N1oiLCJzdGF0dXMiOiJ0cmlnZ2VyZWQiLCJodG1sX3VybCI6Imh0dHBzOi8vc25vd3Bsb3cucGFnZXJkdXR5LmNvbS9pbmNpZGVudHMvUDlXWTlVOSIsImluY2lkZW50X2tleSI6InNydjAxL0hUVFAiLCJzZXJ2aWNlIjp7ImlkIjoiUFRITzRGRiIsIm5hbWUiOiJXZWJob29rcyBUZXN0IiwiaHRtbF91cmwiOiJodHRwczovL3Nub3dwbG93LnBhZ2VyZHV0eS5jb20vc2VydmljZXMvUFRITzRGRiIsImRlbGV0ZWRfYXQiOm51bGx9LCJlc2NhbGF0aW9uX3BvbGljeSI6eyJpZCI6IlA4RVRWSFUiLCJuYW1lIjoiRGVmYXVsdCIsImRlbGV0ZWRfYXQiOm51bGx9LCJhc3NpZ25lZF90b191c2VyIjp7ImlkIjoiUDlMNDI2WCIsIm5hbWUiOiJZYWxpIFNhc3Nvb24iLCJlbWFpbCI6InlhbGlAc25vd3Bsb3dhbmFseXRpY3MuY29tIiwiaHRtbF91cmwiOiJodHRwczovL3Nub3dwbG93LnBhZ2VyZHV0eS5jb20vdXNlcnMvUDlMNDI2WCJ9LCJ0cmlnZ2VyX3N1bW1hcnlfZGF0YSI6eyJkZXNjcmlwdGlvbiI6IkZBSUxVUkUgZm9yIHByb2R1Y3Rpb24vSFRUUCBvbiBtYWNoaW5lIHNydjAxLmFjbWUuY29tIiwiY2xpZW50IjoiU2FtcGxlIE1vbml0b3JpbmcgU2VydmljZSIsImNsaWVudF91cmwiOiJodHRwczovL21vbml0b3Jpbmcuc2VydmljZS5jb20ifSwidHJpZ2dlcl9kZXRhaWxzX2h0bWxfdXJsIjoiaHR0cHM6Ly9zbm93cGxvdy5wYWdlcmR1dHkuY29tL2luY2lkZW50cy9QOVdZOVU5L2xvZ19lbnRyaWVzL1A1QVdQVFIiLCJ0cmlnZ2VyX3R5cGUiOiJ0cmlnZ2VyX3N2Y19ldmVudCIsImxhc3Rfc3RhdHVzX2NoYW5nZV9vbiI6IjIwMTQtMTEtMTJUMTg6NTM6NDdaIiwibGFzdF9zdGF0dXNfY2hhbmdlX2J5IjpudWxsLCJudW1iZXJfb2ZfZXNjYWxhdGlvbnMiOjAsImFzc2lnbmVkX3RvIjpbeyJhdCI6IjIwMTQtMTEtMTJUMTg6NTM6NDdaIiwib2JqZWN0Ijp7ImlkIjoiUDlMNDI2WCIsIm5hbWUiOiJZYWxpIFNhc3Nvb24iLCJlbWFpbCI6InlhbGlAc25vd3Bsb3dhbmFseXRpY3MuY29tIiwiaHRtbF91cmwiOiJodHRwczovL3Nub3dwbG93LnBhZ2VyZHV0eS5jb20vdXNlcnMvUDlMNDI2WCIsInR5cGUiOiJ1c2VyIn19XX19LCJpZCI6IjNjM2U4ZWUwLTZhOWQtMTFlNC1iM2Q1LTIyMDAwYWUzMTM2MSIsImNyZWF0ZWRfb24iOiIyMDE0LTExLTEyVDE4OjUzOjQ3WiJ9XX0="
    )

  val expected = List(
    "crons",
    "srv",
    EtlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.pagerduty-v1",
    "clj-0.6.0-tom-0.0.4",
    EtlVersion,
    null, // No user_id set
    "255.255.x.x",
    null,
    null,
    null,
    "-", // TODO: fix this, https://github.com/snowplow/snowplow/issues/1133
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47Z","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}}}""",
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
    )
}

/**
 * Integration test for the EtlJob:
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class CljTomcatPagerdutyEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing a PagerDuty POST raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatPagerdutyEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 completed call" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CljTomcatPagerdutyEventSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CljTomcatPagerdutyEventSpec.expected(idx), withIndex = idx)
          }
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}
