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

object ApiRequestEnrichmentCfLineSpec {
  import EnrichJobSpec._
  val contexts = """eyJkYXRhIjpbeyJkYXRhIjp7Im9zVHlwZSI6Ik9TWCIsImFwcGxlSWRmdiI6InNvbWVfYXBwbGVJZGZ2Iiwib3BlbklkZmEiOiJzb21lX0lkZmEiLCJjYXJyaWVyIjoic29tZV9jYXJyaWVyIiwiZGV2aWNlTW9kZWwiOiJsYXJnZSIsIm9zVmVyc2lvbiI6IjMuMC4wIiwiYXBwbGVJZGZhIjoic29tZV9hcHBsZUlkZmEiLCJhbmRyb2lkSWRmYSI6InNvbWVfYW5kcm9pZElkZmEiLCJkZXZpY2VNYW51ZmFjdHVyZXIiOiJBbXN0cmFkIn0sInNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L21vYmlsZV9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAifSx7ImRhdGEiOnsibG9uZ2l0dWRlIjoxMCwiYmVhcmluZyI6NTAsInNwZWVkIjoxNiwiYWx0aXR1ZGUiOjIwLCJhbHRpdHVkZUFjY3VyYWN5IjowLjMsImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOjAuNSwibGF0aXR1ZGUiOjd9LCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9nZW9sb2NhdGlvbl9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAifV0sInNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L2NvbnRleHRzL2pzb25zY2hlbWEvMS0wLTAifQ=="""
  val lines = Lines(
    s"2012-05-27  11:35:53  DFW3  3343  70.46.123.145 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=ue&cx=$contexts&ue_pr=%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow%2Funstruct_event%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow-website%2Fsignup_form_submitted%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22name%22%3A%22Bob%C2%AE%22%2C%22email%22%3A%22alex%2Btest%40snowplowanalytics.com%22%2C%22company%22%3A%22SP%22%2C%22eventsPerMonth%22%3A%22%3C%201%20million%22%2C%22serviceType%22%3A%22unsure%22%7D%7D%7D&dtm=1364230969450&evn=com.acme&tid=598951&vp=2560x934&ds=2543x1420&vid=43&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2F&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2F2-tarot-cards"
  )
  val expected = List(
    "pbzsite",
    "web",
    etlTimestamp,
    "2012-05-27 11:35:53.000",
    "2013-03-25 17:02:49.450",
    "unstruct",
    null, // We can't predict the event_id
    "598951",
    null, // No tracker namespace
    "js-0.11.1",
    "cloudfront",
    etlVersion,
    null, // No user_id set
    "70.46.123.145",
    "2876815413",
    "9795bd0203804cd1",
    "43",
    null, // No network_userid set
    "US", // US geolocation
    "FL",
    "Delray Beach",
    null,
    "26.461502",
    "-80.0728",
    "Florida",
    null,
    null,
    "nuvox.net",  // Using the MaxMind domain lookup service
    null,
    "http://www.psychicbazaar.com/2-tarot-cards",
    null, // No page title for events
    "http://www.psychicbazaar.com/",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards",
    null,
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/",
    null,
    null,
    "internal", // Internal referer
    null,
    null,
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    """{"data":[{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""", // Custom context
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0","data":{"name":"Bob®","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}}}""", // Unstructured event field set
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
    null, // Page ping fields are empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0",
    "Firefox 12",
    "Firefox",
    "12.0",
    "Browser",
    "GECKO",
    "en-GB",
    "1",
    "1",
    "1",
    "0",
    "0",
    "0",
    "0",
    "0",
    "1",
    "1",
    "32",
    "2560",
    "934",
    "Windows 7",
    "Windows",
    "Microsoft Corporation",
    "Europe/London",
    "Computer",
    "0",
    "2560",
    "1440",
    "UTF-8",
    "2543",
    "1420",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    "America/New_York",
    null,
    null,
    null,
    null,
    null,
    null,
    """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/user/jsonschema/1-0-0","data":{"path":"/guest/users/large/Windows+7/www.psychicbazaar.com/unsure?format=json","message":"unauthorized","method":"GET"}},{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Firefox","useragentMajor":"12","useragentMinor":"0","useragentPatch":null,"useragentVersion":"Firefox 12.0","osFamily":"Windows 7","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Windows 7","deviceFamily":"Other"}}]}""",
    null,
    "2012-05-27 11:35:53.000",
    "com.snowplowanalytics.snowplow-website",
    "signup_form_submitted",
    "jsonschema",
    "1-0-0",
    "2e1b9815ade90b8d4da30f221db6704a"
  )

  /** Check if test running in CI environment */
  def continuousIntegration: Boolean = sys.env.get("CI") match {
    case Some("true") => true
    case _ => false
  }
}

/** Check API Request Enrichment makes request and attaches context to event */
class ApiRequestEnrichmentCfLineSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "api-request-enrichment-cf-lines"
  sequential
  // Only run API Request Enrichment test if the its started on CI and api-request-test.py is running
  if (ApiRequestEnrichmentCfLineSpec.continuousIntegration) {
    "A job which processes a CloudFront file containing 1 valid custom unstructured event, " +
    "derived context and custom contexts" should {
      runEnrichJob(ApiRequestEnrichmentCfLineSpec.lines, "cloudfront", "1", false,
        List("geo", "domain"), apiRequestEnabled = true)

      "correctly attach derived context fetched from REST server" in {
        val Some(goods) = readPartFile(dirs.output)
        goods.size must_== 1
        val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
        for (idx <- ApiRequestEnrichmentCfLineSpec.expected.indices) {
          actual(idx) must
            beFieldEqualTo(ApiRequestEnrichmentCfLineSpec.expected(idx), idx)
        }
      }

      "not write any bad rows" in {
        dirs.badRows must beEmptyDir
      }
    }
  } else {
    println("WARNING: Skipping APIRequestEnrichmentCfLineSpec as no CI=true environment variable " +
      "was found and integration-tests/api-lookup-test.py probably not running")
  }
}
