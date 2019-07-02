/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam
package adapters

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._
import io.circe.literal._

object PagerDutyAdapterSpec {
  val body =
    json"""{"messages":[{"type":"incident.resolve","data":{"incident":{"id":"P850PWC","incident_number":597,"created_on":"2014-12-17T17:02:51","status":"resolved","html_url":"https://snowplow.pagerduty.com/incidents/P850PWC","incident_key":"/opt/snowplow-saas/bin/run-and-load-goeuro-adwords.shgoeuro@1418835766","service":{"id":"PE7H89B","name":"ManagedServiceBatchYaliCrons","html_url":"https://snowplow.pagerduty.com/services/PE7H89B","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Yalifirst","deleted_at":null},"assigned_to_user":null,"trigger_summary_data":{"description":"executordetectedfailureforgoeuro"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P850PWC/log_entries/Q2KN5OMT7QL5L0","trigger_type":"trigger_svc_event","last_status_change_on":"2014-12-17T17:11:46","last_status_change_by":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"number_of_escalations":0,"resolved_by_user":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"assigned_to":[]}},"id":"c8565510-860f-11e4-bbe8-22000ad9bf74","created_on":"2014-12-17T17:11:46Z"}]}"""
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.pagerduty/v1",
      body = body.noSpaces.some,
      contentType = "application/json".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.pagerduty-v1",
    "event_vendor" -> "com.pagerduty",
    "event_name" -> "incident",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"resolve","data":{"incident":{"assigned_to_user":null,"incident_key":"/opt/snowplow-saas/bin/run-and-load-goeuro-adwords.shgoeuro@1418835766","trigger_summary_data":{"description":"executordetectedfailureforgoeuro"},"last_status_change_by":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"incident_number":597,"resolved_by_user":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"service":{"id":"PE7H89B","name":"ManagedServiceBatchYaliCrons","html_url":"https://snowplow.pagerduty.com/services/PE7H89B","deleted_at":null},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P850PWC/log_entries/Q2KN5OMT7QL5L0","id":"P850PWC","assigned_to":[],"number_of_escalations":0,"last_status_change_on":"2014-12-17T17:11:46","status":"resolved","escalation_policy":{"id":"P8ETVHU","name":"Yalifirst","deleted_at":null},"created_on":"2014-12-17T17:02:51","trigger_type":"trigger_svc_event","html_url":"https://snowplow.pagerduty.com/incidents/P850PWC"}},"id":"c8565510-860f-11e4-bbe8-22000ad9bf74","created_on":"2014-12-17T17:11:46Z"}}}""".noSpaces
  )
}

class PagerDutyAdapterSpec extends PipelineSpec {
  import PagerDutyAdapterSpec._
  "PagerDutyAdapter" should "enrich using the pager duty adapter" in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(DistCacheIO(""), List.empty[Either[String, String]])
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
