/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package good

import org.specs2.mutable.Specification

object CljTomcatPagerdutyEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.pagerduty/v1   404 -  -    aid=crons&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fjson   ew0KICAgICJtZXNzYWdlcyI6IFsNCiAgICAgICAgew0KICAgICAgICAgICAgInR5cGUiOiAiaW5jaWRlbnQucmVzb2x2ZSIsDQogICAgICAgICAgICAiZGF0YSI6IHsNCiAgICAgICAgICAgICAgICAiaW5jaWRlbnQiOiB7DQogICAgICAgICAgICAgICAgICAgICJpZCI6ICJQODUwUFdDIiwNCiAgICAgICAgICAgICAgICAgICAgImluY2lkZW50X251bWJlciI6IDU5NywNCiAgICAgICAgICAgICAgICAgICAgImNyZWF0ZWRfb24iOiAiMjAxNC0xMi0xN1QxNzowMjo1MSAwMDowMCIsDQogICAgICAgICAgICAgICAgICAgICJzdGF0dXMiOiAicmVzb2x2ZWQiLA0KICAgICAgICAgICAgICAgICAgICAiaHRtbF91cmwiOiAiaHR0cHM6Ly9zbm93cGxvdy5wYWdlcmR1dHkuY29tL2luY2lkZW50cy9QODUwUFdDIiwNCiAgICAgICAgICAgICAgICAgICAgImluY2lkZW50X2tleSI6ICIvb3B0L3Nub3dwbG93LXNhYXMvYmluL3J1bi1hbmQtbG9hZC1nb2V1cm8tYWR3b3Jkcy5zaCBnb2V1cm8gQCAxNDE4ODM1NzY2IiwNCiAgICAgICAgICAgICAgICAgICAgInNlcnZpY2UiOiB7DQogICAgICAgICAgICAgICAgICAgICAgICAiaWQiOiAiUEU3SDg5QiIsDQogICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICJNYW5hZ2VkIFNlcnZpY2UgQmF0Y2ggWWFsaSBDcm9ucyIsDQogICAgICAgICAgICAgICAgICAgICAgICAiaHRtbF91cmwiOiAiaHR0cHM6Ly9zbm93cGxvdy5wYWdlcmR1dHkuY29tL3NlcnZpY2VzL1BFN0g4OUIiLA0KICAgICAgICAgICAgICAgICAgICAgICAgImRlbGV0ZWRfYXQiOiBudWxsDQogICAgICAgICAgICAgICAgICAgIH0sDQogICAgICAgICAgICAgICAgICAgICJlc2NhbGF0aW9uX3BvbGljeSI6IHsNCiAgICAgICAgICAgICAgICAgICAgICAgICJpZCI6ICJQOEVUVkhVIiwNCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIllhbGkgZmlyc3QiLA0KICAgICAgICAgICAgICAgICAgICAgICAgImRlbGV0ZWRfYXQiOiBudWxsDQogICAgICAgICAgICAgICAgICAgIH0sDQogICAgICAgICAgICAgICAgICAgICJhc3NpZ25lZF90b191c2VyIjogbnVsbCwNCiAgICAgICAgICAgICAgICAgICAgInRyaWdnZXJfc3VtbWFyeV9kYXRhIjogew0KICAgICAgICAgICAgICAgICAgICAgICAgImRlc2NyaXB0aW9uIjogImV4ZWN1dG9yIGRldGVjdGVkIGZhaWx1cmUgZm9yIGdvZXVybyINCiAgICAgICAgICAgICAgICAgICAgfSwNCiAgICAgICAgICAgICAgICAgICAgInRyaWdnZXJfZGV0YWlsc19odG1sX3VybCI6ICJodHRwczovL3Nub3dwbG93LnBhZ2VyZHV0eS5jb20vaW5jaWRlbnRzL1A4NTBQV0MvbG9nX2VudHJpZXMvUTJLTjVPTVQ3UUw1TDAiLA0KICAgICAgICAgICAgICAgICAgICAidHJpZ2dlcl90eXBlIjogInRyaWdnZXJfc3ZjX2V2ZW50IiwNCiAgICAgICAgICAgICAgICAgICAgImxhc3Rfc3RhdHVzX2NoYW5nZV9vbiI6ICIyMDE0LTEyLTE3VDE3OjExOjQ2IDAwOjAwIiwNCiAgICAgICAgICAgICAgICAgICAgImxhc3Rfc3RhdHVzX2NoYW5nZV9ieSI6IHsNCiAgICAgICAgICAgICAgICAgICAgICAgICJpZCI6ICJQOUw0MjZYIiwNCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIllhbGkgU2Fzc29vbiIsDQogICAgICAgICAgICAgICAgICAgICAgICAiZW1haWwiOiAieWFsaUBzbm93cGxvd2FuYWx5dGljcy5jb20iLA0KICAgICAgICAgICAgICAgICAgICAgICAgImh0bWxfdXJsIjogImh0dHBzOi8vc25vd3Bsb3cucGFnZXJkdXR5LmNvbS91c2Vycy9QOUw0MjZYIg0KICAgICAgICAgICAgICAgICAgICB9LA0KICAgICAgICAgICAgICAgICAgICAibnVtYmVyX29mX2VzY2FsYXRpb25zIjogMCwNCiAgICAgICAgICAgICAgICAgICAgInJlc29sdmVkX2J5X3VzZXIiOiB7DQogICAgICAgICAgICAgICAgICAgICAgICAiaWQiOiAiUDlMNDI2WCIsDQogICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICJZYWxpIFNhc3Nvb24iLA0KICAgICAgICAgICAgICAgICAgICAgICAgImVtYWlsIjogInlhbGlAc25vd3Bsb3dhbmFseXRpY3MuY29tIiwNCiAgICAgICAgICAgICAgICAgICAgICAgICJodG1sX3VybCI6ICJodHRwczovL3Nub3dwbG93LnBhZ2VyZHV0eS5jb20vdXNlcnMvUDlMNDI2WCINCiAgICAgICAgICAgICAgICAgICAgfSwNCiAgICAgICAgICAgICAgICAgICAgImFzc2lnbmVkX3RvIjogW10NCiAgICAgICAgICAgICAgICB9DQogICAgICAgICAgICB9LA0KICAgICAgICAgICAgImlkIjogImM4NTY1NTEwLTg2MGYtMTFlNC1iYmU4LTIyMDAwYWQ5YmY3NCIsDQogICAgICAgICAgICAiY3JlYXRlZF9vbiI6ICIyMDE0LTEyLTE3VDE3OjExOjQ2WiINCiAgICAgICAgfQ0KICAgIF0NCn0="
  )
  val expected = List(
    "crons",
    "srv",
    etlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.pagerduty-v1",
    "clj-0.6.0-tom-0.0.4",
    etlVersion,
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"resolve","data":{"incident":{"id":"P850PWC","incident_number":597,"created_on":"2014-12-17T17:02:51+00:00","status":"resolved","html_url":"https://snowplow.pagerduty.com/incidents/P850PWC","incident_key":"/opt/snowplow-saas/bin/run-and-load-goeuro-adwords.sh goeuro @ 1418835766","service":{"id":"PE7H89B","name":"Managed Service Batch Yali Crons","html_url":"https://snowplow.pagerduty.com/services/PE7H89B","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Yali first","deleted_at":null},"assigned_to_user":null,"trigger_summary_data":{"description":"executor detected failure for goeuro"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P850PWC/log_entries/Q2KN5OMT7QL5L0","trigger_type":"trigger_svc_event","last_status_change_on":"2014-12-17T17:11:46+00:00","last_status_change_by":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"number_of_escalations":0,"resolved_by_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"assigned_to":[]}},"id":"c8565510-860f-11e4-bbe8-22000ad9bf74","created_on":"2014-12-17T17:11:46Z"}}}""",
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

class CljTomcatPagerdutyEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-pagerduty-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a PagerDuty POST raw event representing" +
  " 1 valid completed call" should {
    runEnrichJob(CljTomcatPagerdutyEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatPagerdutyEventSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(CljTomcatPagerdutyEventSpec.expected(idx), idx)
      }

      "not write any bad rows" in {
        dirs.badRows must beEmptyDir
      }
    }
  }
}
