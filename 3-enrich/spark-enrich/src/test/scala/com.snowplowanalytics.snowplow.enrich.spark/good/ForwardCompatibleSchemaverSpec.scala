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

object ForwardCompatibleSchemaverSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-09-08 13:58:56  - - 37.157.33.178 POST  - /com.snowplowanalytics.snowplow/tp2 200 - python-requests%2F2.2.1+CPython%2F3.3.5+Linux%2F3.2.0-61-generic  &cv=clj-0.7.0-tom-0.1.0&nuid=f732d278-120e-4ab6-845b-c1f11cd85dc7 - - - application%2Fjson%3B+charset%3Dutf-8 eyJkYXRhIjogW3siZSI6ICJwdiIsICJ0diI6ICJweS0wLjUuMCIsICJwIjogInBjIiwgImR0bSI6ICIxNDEwMTg0NzM2NzgzIiwgInVybCI6ICJodHRwOi8vd3d3LmV4YW1wbGUuY29tIiwgImN4IjogImV5SmtZWFJoSWpvZ1czc2laR0YwWVNJNklIc2liM05VZVhCbElqb2dJazlUV0NJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNCbGJrbGtabUVpT2lBaWMyOXRaVjlKWkdaaElpd2dJbU5oY25KcFpYSWlPaUFpYzI5dFpWOWpZWEp5YVdWeUlpd2dJbVJsZG1salpVMXZaR1ZzSWpvZ0lteGhjbWRsSWl3Z0ltOXpWbVZ5YzJsdmJpSTZJQ0l6TGpBdU1DSXNJQ0poY0hCc1pVbGtabUVpT2lBaWMyOXRaVjloY0hCc1pVbGtabUVpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOWVc1MVptRmpkSFZ5WlhJaU9pQWlRVzF6ZEhKaFpDSjlMQ0FpYzJOb1pXMWhJam9nSW1sbmJIVTZZMjl0TG5OdWIzZHdiRzkzWVc1aGJIbDBhV056TG5OdWIzZHdiRzkzTDIxdlltbHNaVjlqYjI1MFpYaDBMMnB6YjI1elkyaGxiV0V2TVMwd0xUQWlmU3dnZXlKa1lYUmhJam9nZXlKc2IyNW5hWFIxWkdVaU9pQXhNQ3dnSW1KbFlYSnBibWNpT2lBMU1Dd2dJbk53WldWa0lqb2dNVFlzSUNKaGJIUnBkSFZrWlNJNklESXdMQ0FpWVd4MGFYUjFaR1ZCWTJOMWNtRmplU0k2SURBdU15d2dJbXhoZEdsMGRXUmxURzl1WjJsMGRXUmxRV05qZFhKaFkza2lPaUF3TGpVc0lDSnNZWFJwZEhWa1pTSTZJRGQ5TENBaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyZGxiMnh2WTJGMGFXOXVYMk52Ym5SbGVIUXZhbk52Ym5OamFHVnRZUzh4TFRBdE1DSjlYU3dnSW5OamFHVnRZU0k2SUNKcFoyeDFPbU52YlM1emJtOTNjR3h2ZDJGdVlXeDVkR2xqY3k1emJtOTNjR3h2ZHk5amIyNTBaWGgwY3k5cWMyOXVjMk5vWlcxaEx6RXRNQzB3SW4wPSIsICJlaWQiOiAiYTkxYTMxYmMtNTkzYi00ODFmLTg2OWEtOWQ4MjIwMDBlZTg2In0sIHsic2VfY2EiOiAibXlfY2F0ZWdvcnkiLCAic2VfYWMiOiAibXlfYWN0aW9uIiwgImUiOiAic2UiLCAidHYiOiAicHktMC41LjAiLCAicCI6ICJwYyIsICJkdG0iOiAiMTQxMDE4NDczNjc4NCIsICJjeCI6ICJleUprWVhSaElqb2dXM3NpWkdGMFlTSTZJSHNpYjNOVWVYQmxJam9nSWs5VFdDSXNJQ0poY0hCc1pVbGtabllpT2lBaWMyOXRaVjloY0hCc1pVbGtabllpTENBaWIzQmxia2xrWm1FaU9pQWljMjl0WlY5SlpHWmhJaXdnSW1OaGNuSnBaWElpT2lBaWMyOXRaVjlqWVhKeWFXVnlJaXdnSW1SbGRtbGpaVTF2WkdWc0lqb2dJbXhoY21kbElpd2dJbTl6Vm1WeWMybHZiaUk2SUNJekxqQXVNQ0lzSUNKaGNIQnNaVWxrWm1FaU9pQWljMjl0WlY5aGNIQnNaVWxrWm1FaUxDQWlZVzVrY205cFpFbGtabUVpT2lBaWMyOXRaVjloYm1SeWIybGtTV1JtWVNJc0lDSmtaWFpwWTJWTllXNTFabUZqZEhWeVpYSWlPaUFpUVcxemRISmhaQ0o5TENBaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyMXZZbWxzWlY5amIyNTBaWGgwTDJwemIyNXpZMmhsYldFdk1TMHdMVEFpZlN3Z2V5SmtZWFJoSWpvZ2V5SnNiMjVuYVhSMVpHVWlPaUF4TUN3Z0ltSmxZWEpwYm1jaU9pQTFNQ3dnSW5Od1pXVmtJam9nTVRZc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaVlXeDBhWFIxWkdWQlkyTjFjbUZqZVNJNklEQXVNeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0pzWVhScGRIVmtaU0k2SURkOUxDQWljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMmRsYjJ4dlkyRjBhVzl1WDJOdmJuUmxlSFF2YW5OdmJuTmphR1Z0WVM4eExUQXRNQ0o5WFN3Z0luTmphR1Z0WVNJNklDSnBaMngxT21OdmJTNXpibTkzY0d4dmQyRnVZV3g1ZEdsamN5NXpibTkzY0d4dmR5OWpiMjUwWlhoMGN5OXFjMjl1YzJOb1pXMWhMekV0TUMwd0luMD0iLCAiZWlkIjogIjAwZGE5ZTYyLThhMWMtNGZmYy05MmE1LWJjMGQ0ZmI3OTdhOSJ9LCB7InNlX2NhIjogImFub3RoZXJfY2F0ZWdvcnkiLCAic2VfYWMiOiAiYW5vdGhlcl9hY3Rpb24iLCAiZSI6ICJzZSIsICJ0diI6ICJweS0wLjUuMCIsICJwIjogInBjIiwgImR0bSI6ICIxNDEwMTg0NzM2Nzg0IiwgImN4IjogImV5SmtZWFJoSWpvZ1czc2laR0YwWVNJNklIc2liM05VZVhCbElqb2dJazlUV0NJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNCbGJrbGtabUVpT2lBaWMyOXRaVjlKWkdaaElpd2dJbU5oY25KcFpYSWlPaUFpYzI5dFpWOWpZWEp5YVdWeUlpd2dJbVJsZG1salpVMXZaR1ZzSWpvZ0lteGhjbWRsSWl3Z0ltOXpWbVZ5YzJsdmJpSTZJQ0l6TGpBdU1DSXNJQ0poY0hCc1pVbGtabUVpT2lBaWMyOXRaVjloY0hCc1pVbGtabUVpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOWVc1MVptRmpkSFZ5WlhJaU9pQWlRVzF6ZEhKaFpDSjlMQ0FpYzJOb1pXMWhJam9nSW1sbmJIVTZZMjl0TG5OdWIzZHdiRzkzWVc1aGJIbDBhV056TG5OdWIzZHdiRzkzTDIxdlltbHNaVjlqYjI1MFpYaDBMMnB6YjI1elkyaGxiV0V2TVMwd0xUQWlmU3dnZXlKa1lYUmhJam9nZXlKc2IyNW5hWFIxWkdVaU9pQXhNQ3dnSW1KbFlYSnBibWNpT2lBMU1Dd2dJbk53WldWa0lqb2dNVFlzSUNKaGJIUnBkSFZrWlNJNklESXdMQ0FpWVd4MGFYUjFaR1ZCWTJOMWNtRmplU0k2SURBdU15d2dJbXhoZEdsMGRXUmxURzl1WjJsMGRXUmxRV05qZFhKaFkza2lPaUF3TGpVc0lDSnNZWFJwZEhWa1pTSTZJRGQ5TENBaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyZGxiMnh2WTJGMGFXOXVYMk52Ym5SbGVIUXZhbk52Ym5OamFHVnRZUzh4TFRBdE1DSjlYU3dnSW5OamFHVnRZU0k2SUNKcFoyeDFPbU52YlM1emJtOTNjR3h2ZDJGdVlXeDVkR2xqY3k1emJtOTNjR3h2ZHk5amIyNTBaWGgwY3k5cWMyOXVjMk5vWlcxaEx6RXRNQzB3SW4wPSIsICJlaWQiOiAiMTE1ZDU2YzUtY2UwYS00ZmJlLWE2NGYtODMwYmMyMWVmZmI4In1dLCAic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L3BheWxvYWRfZGF0YS9qc29uc2NoZW1hLzEtMC0yIn0="
  )

  val expected = {
    val platform         = "pc"
    val collector_tstamp = "2014-09-08 13:58:56.000"
    val v_tracker        = "py-0.5.0"
    val v_collector      = "clj-0.7.0-tom-0.1.0"
    val user_ipaddress   = "37.157.x.x"
    val contexts         = """{"data":[{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}"""
    val useragent        = "python-requests/2.2.1 CPython/3.3.5 Linux/3.2.0-61-generic"
    val br_name          = "Unknown"
    val br_family        = "Unknown"
    val br_type          = "unknown"
    val br_renderengine  = "OTHER"
    val os_name          = "Linux"
    val os_family        = "Linux"
    val os_manufacturer  = "Other"
    val dvce_type        = "Computer"
    val dvce_ismobile    = "0"

    // 3 events
    List(
      // First event
      List(
        null,
        platform,
        etlTimestamp,
        collector_tstamp,
        "2014-09-08 13:58:56.783",
        "page_view",
        null, // TODO: we actually can predict event_id as set using eid
        null, // No transaction ID
        null, // No tracker namespace
        v_tracker,
        v_collector,
        etlVersion,
        null, // No user_id set
        user_ipaddress,
        null,
        null,
        null,
        "f732d278-120e-4ab6-845b-c1f11cd85dc7",
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
        "http://www.example.com",
        null,
        null,
        "http",
        "www.example.com",
        "80",
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
        contexts,
        null, // Structured event fields empty
        null, //
        null, //
        null, //
        null, //
        null, // Unstructured event field empty
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
        useragent,
        br_name,
        br_family,
        null,
        br_type,
        br_renderengine,
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
        os_name,
        os_family,
        os_manufacturer,
        null,
        dvce_type,
        dvce_ismobile,
        null,
        null,
        null,
        null,
        null
      ),
      // Second event
      List(
        null,
        platform,
        etlTimestamp,
        collector_tstamp,
        "2014-09-08 13:58:56.784",
        "struct",
        null, // TODO: we actually can predict event_id as set using eid
        null, // No transaction ID
        null, // No tracker namespace
        v_tracker,
        v_collector,
        etlVersion,
        null, // No user_id set
        user_ipaddress,
        null,
        null,
        null,
        "f732d278-120e-4ab6-845b-c1f11cd85dc7",
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
        contexts,
        "my_category",
        "my_action",
        null,
        null,
        null,
        null, // Unstructured event field empty
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
        useragent,
        br_name,
        br_family,
        null,
        br_type,
        br_renderengine,
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
        os_name,
        os_family,
        os_manufacturer,
        null,
        dvce_type,
        dvce_ismobile,
        null,
        null,
        null,
        null,
        null
      ),
      // Third event
      List(
        null,
        platform,
        etlTimestamp,
        collector_tstamp,
        "2014-09-08 13:58:56.784",
        "struct",
        null, // TODO: we actually can predict event_id as set using eid
        null, // No transaction ID
        null, // No tracker namespace
        v_tracker,
        v_collector,
        etlVersion,
        null, // No user_id set
        user_ipaddress,
        null,
        null,
        null,
        "f732d278-120e-4ab6-845b-c1f11cd85dc7",
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
        contexts,
        "another_category",
        "another_action",
        null, //
        null, //
        null, //
        null, // Unstructured event field empty
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
        useragent,
        br_name,
        br_family,
        null,
        br_type,
        br_renderengine,
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
        os_name,
        os_family,
        os_manufacturer,
        null,
        dvce_type,
        dvce_ismobile,
        null,
        null,
        null,
        null,
        null
      )
    )
  }
}

/**
 * Check that a payload_data JSON with schema version 1-0-2 passes validation
 * See https://github.com/snowplow/iglu-scala-client/issues/21
 */
class ForwardCompatibleSchemaverSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "forwarrd-compatbile-schemaver"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing " +
  "3 events" should {
    runEnrichJob(ForwardCompatibleSchemaverSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 page view and 2 structured events" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 3
      for {
        ei <- ForwardCompatibleSchemaverSpec.expected.indices
        fi <- ForwardCompatibleSchemaverSpec.expected(ei).indices
      } {
        goods(ei).split("\t").map(s => if (s.isEmpty()) null else s).apply(fi) must
          beFieldEqualTo(ForwardCompatibleSchemaverSpec.expected(ei)(fi), fi)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
