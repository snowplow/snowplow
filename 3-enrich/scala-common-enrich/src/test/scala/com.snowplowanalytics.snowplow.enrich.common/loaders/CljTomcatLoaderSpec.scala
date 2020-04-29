/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package loaders

import cats.data.NonEmptyList
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import org.joda.time.DateTime
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import SpecHelpers._

class CljTomcatLoaderSpec extends Specification with DataTables with ValidatedMatchers with ScalaCheck {
  val Process = Processor("CljTomcatLoaderSpec", "v1")

  def is = s2"""
  toCollectorPayload should return a CanonicalInput for a valid raw event                                             $e1
  toCollectorPayload should return a Validation Failure for a log record with body but with operation other than POST $e2
  toCollectorPayload should return a Validation Failure for a POST log record with corrupted API vendor/version       $e3
  toCollectorPayload should return a Validation Failure for an unparseable Clj-Tomcat log record                      $e4
  """

  object Expected {
    val collector = "clj-tomcat"
    val encoding = "UTF-8"
    val vendor = "com.snowplowanalytics.snowplow"
    val ipAddress = "37.157.33.123".some
  }

  def e1 =
    "SPEC NAME" || "RAW" | "EXP. VERSION" | "EXP. PAYLOAD" | "EXP. CONTENT TYPE" | "EXP. BODY" | "EXP. TIMESTAMP" | "EXP. USER AGENT" | "EXP. REFERER URI" |
      "Snowplow Tp1 GET w/ v0.6.0 collector" !! "2013-08-29  00:18:48  -  830 37.157.33.123 GET d3v6ndkyapxc2w.cloudfront.net /i  200 http://snowplowanalytics.com/analytics/index.html Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0 e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html - - -" !
        "tp1" ! toNameValuePairs(
        "e" -> "pv",
        "page" -> "Introduction - Snowplow Analytics%",
        "dtm" -> "1377735557970",
        "tid" -> "567074",
        "vp" -> "1024x635",
        "ds" -> "1024x635",
        "vid" -> "1",
        "duid" -> "7969620089de36eb",
        "p" -> "web",
        "tv" -> "js-0.12.0",
        "fp" -> "308909339",
        "aid" -> "snowplowweb",
        "lang" -> "en-US",
        "cs" -> "UTF-8",
        "tz" -> "America/Los_Angeles",
        "refr" -> "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
        "f_pdf" -> "1",
        "f_qt" -> "1",
        "f_realp" -> "0",
        "f_wma" -> "1",
        "f_dir" -> "0",
        "f_fla" -> "1",
        "f_java" -> "1",
        "f_gears" -> "0",
        "f_ag" -> "0",
        "res" -> "1024x768",
        "cd" -> "24",
        "cookie" -> "1",
        "url" -> "http://snowplowanalytics.com/analytics/index.html"
      ) ! None ! None ! DateTime.parse("2013-08-29T00:18:48.000+00:00") !
        "Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0".some ! "http://snowplowanalytics.com/analytics/index.html".some |
      "Snowplow Tp1 GET w/ v0.7.0 collector" !! "2013-08-29  00:18:48  -  830 37.157.33.123 GET d3v6ndkyapxc2w.cloudfront.net /i  200 http://snowplowanalytics.com/analytics/index.html Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0 e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html - - - - -" !
        "tp1" ! toNameValuePairs(
        "e" -> "pv",
        "page" -> "Introduction - Snowplow Analytics%",
        "dtm" -> "1377735557970",
        "tid" -> "567074",
        "vp" -> "1024x635",
        "ds" -> "1024x635",
        "vid" -> "1",
        "duid" -> "7969620089de36eb",
        "p" -> "web",
        "tv" -> "js-0.12.0",
        "fp" -> "308909339",
        "aid" -> "snowplowweb",
        "lang" -> "en-US",
        "cs" -> "UTF-8",
        "tz" -> "America/Los_Angeles",
        "refr" -> "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
        "f_pdf" -> "1",
        "f_qt" -> "1",
        "f_realp" -> "0",
        "f_wma" -> "1",
        "f_dir" -> "0",
        "f_fla" -> "1",
        "f_java" -> "1",
        "f_gears" -> "0",
        "f_ag" -> "0",
        "res" -> "1024x768",
        "cd" -> "24",
        "cookie" -> "1",
        "url" -> "http://snowplowanalytics.com/analytics/index.html"
      ) ! None ! None ! DateTime.parse("2013-08-29T00:18:48.000+00:00") !
        "Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0".some ! "http://snowplowanalytics.com/analytics/index.html".some |
      "Snowplow Tp2 POST w/ v0.6.0 collector" !! "2014-09-08 13:59:07  - - 37.157.33.123 POST  - /com.snowplowanalytics.snowplow/tp2 200 - python-requests%2F2.2.1+CPython%2F3.3.5+Linux%2F3.2.0-61-generic  &cv=clj-0.7.0-tom-0.1.0&nuid=5c6c40e4-eff8-409b-9327-471f303e30b6 - - - application%2Fjson%3B+charset%3Dutf-8 eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvcGF5bG9hZF9kYXRhL2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IFt7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAicHYiLCAiZWlkIjogIjJjYWU0MTkxLTMxY2QtNDc4My04MmE4LWRmNTMxOGY0NGFmZiIsICJ1cmwiOiAiaHR0cDovL3d3dy5leGFtcGxlLmNvbSIsICJ0diI6ICJweS0wLjUuMCIsICJjeCI6ICJleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WTI5dWRHVjRkSE12YW5OdmJuTmphR1Z0WVM4eExUQXRNQ0lzSUNKa1lYUmhJam9nVzNzaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyMXZZbWxzWlY5amIyNTBaWGgwTDJwemIyNXpZMmhsYldFdk1TMHdMVEFpTENBaVpHRjBZU0k2SUhzaVpHVjJhV05sVFdGdWRXWmhZM1IxY21WeUlqb2dJa0Z0YzNSeVlXUWlMQ0FpWVc1a2NtOXBaRWxrWm1FaU9pQWljMjl0WlY5aGJtUnliMmxrU1dSbVlTSXNJQ0prWlhacFkyVk5iMlJsYkNJNklDSnNZWEpuWlNJc0lDSnZjR1Z1U1dSbVlTSTZJQ0p6YjIxbFgwbGtabUVpTENBaVkyRnljbWxsY2lJNklDSnpiMjFsWDJOaGNuSnBaWElpTENBaVlYQndiR1ZKWkdaaElqb2dJbk52YldWZllYQndiR1ZKWkdaaElpd2dJbTl6Vm1WeWMybHZiaUk2SUNJekxqQXVNQ0lzSUNKaGNIQnNaVWxrWm5ZaU9pQWljMjl0WlY5aGNIQnNaVWxrWm5ZaUxDQWliM05VZVhCbElqb2dJazlUV0NKOWZTd2dleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WjJWdmJHOWpZWFJwYjI1ZlkyOXVkR1Y0ZEM5cWMyOXVjMk5vWlcxaEx6RXRNQzB3SWl3Z0ltUmhkR0VpT2lCN0lteHZibWRwZEhWa1pTSTZJREV3TENBaVlXeDBhWFIxWkdWQlkyTjFjbUZqZVNJNklEQXVNeXdnSW14aGRHbDBkV1JsSWpvZ055d2dJbXhoZEdsMGRXUmxURzl1WjJsMGRXUmxRV05qZFhKaFkza2lPaUF3TGpVc0lDSmlaV0Z5YVc1bklqb2dOVEFzSUNKaGJIUnBkSFZrWlNJNklESXdMQ0FpYzNCbFpXUWlPaUF4Tm4xOVhYMD0iLCAicCI6ICJwYyJ9LCB7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAic2UiLCAiZWlkIjogIjVhNzExODg1LTY5ZGMtNGY0Mi04Nzg1LWZjNjVmMTc1OGVjMCIsICJzZV9hYyI6ICJteV9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogIm15X2NhdGVnb3J5IiwgInAiOiAicGMifSwgeyJkdG0iOiAiMTQxMDE4NDc0Njg5NSIsICJlIjogInNlIiwgImVpZCI6ICI4M2VhYzIyNy03MTI5LTQyYTctYWY0NS00MGY2M2VkNGI5ZGQiLCAic2VfYWMiOiAiYW5vdGhlcl9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogImFub3RoZXJfY2F0ZWdvcnkiLCAicCI6ICJwYyJ9XX0" !
        "tp2" ! toNameValuePairs(
        "cv" -> "clj-0.7.0-tom-0.1.0",
        "nuid" -> "5c6c40e4-eff8-409b-9327-471f303e30b6"
      ) ! "application/json; charset=utf-8".some ! """{"schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-0", "data": [{"dtm": "1410184746894", "e": "pv", "eid": "2cae4191-31cd-4783-82a8-df5318f44aff", "url": "http://www.example.com", "tv": "py-0.5.0", "cx": "eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY29udGV4dHMvanNvbnNjaGVtYS8xLTAtMCIsICJkYXRhIjogW3sic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L21vYmlsZV9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IHsiZGV2aWNlTWFudWZhY3R1cmVyIjogIkFtc3RyYWQiLCAiYW5kcm9pZElkZmEiOiAic29tZV9hbmRyb2lkSWRmYSIsICJkZXZpY2VNb2RlbCI6ICJsYXJnZSIsICJvcGVuSWRmYSI6ICJzb21lX0lkZmEiLCAiY2FycmllciI6ICJzb21lX2NhcnJpZXIiLCAiYXBwbGVJZGZhIjogInNvbWVfYXBwbGVJZGZhIiwgIm9zVmVyc2lvbiI6ICIzLjAuMCIsICJhcHBsZUlkZnYiOiAic29tZV9hcHBsZUlkZnYiLCAib3NUeXBlIjogIk9TWCJ9fSwgeyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvZ2VvbG9jYXRpb25fY29udGV4dC9qc29uc2NoZW1hLzEtMC0wIiwgImRhdGEiOiB7ImxvbmdpdHVkZSI6IDEwLCAiYWx0aXR1ZGVBY2N1cmFjeSI6IDAuMywgImxhdGl0dWRlIjogNywgImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOiAwLjUsICJiZWFyaW5nIjogNTAsICJhbHRpdHVkZSI6IDIwLCAic3BlZWQiOiAxNn19XX0=", "p": "pc"}, {"dtm": "1410184746894", "e": "se", "eid": "5a711885-69dc-4f42-8785-fc65f1758ec0", "se_ac": "my_action", "tv": "py-0.5.0", "cx": "eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY29udGV4dHMvanNvbnNjaGVtYS8xLTAtMCIsICJkYXRhIjogW3sic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L21vYmlsZV9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IHsiZGV2aWNlTWFudWZhY3R1cmVyIjogIkFtc3RyYWQiLCAiYW5kcm9pZElkZmEiOiAic29tZV9hbmRyb2lkSWRmYSIsICJkZXZpY2VNb2RlbCI6ICJsYXJnZSIsICJvcGVuSWRmYSI6ICJzb21lX0lkZmEiLCAiY2FycmllciI6ICJzb21lX2NhcnJpZXIiLCAiYXBwbGVJZGZhIjogInNvbWVfYXBwbGVJZGZhIiwgIm9zVmVyc2lvbiI6ICIzLjAuMCIsICJhcHBsZUlkZnYiOiAic29tZV9hcHBsZUlkZnYiLCAib3NUeXBlIjogIk9TWCJ9fSwgeyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvZ2VvbG9jYXRpb25fY29udGV4dC9qc29uc2NoZW1hLzEtMC0wIiwgImRhdGEiOiB7ImxvbmdpdHVkZSI6IDEwLCAiYWx0aXR1ZGVBY2N1cmFjeSI6IDAuMywgImxhdGl0dWRlIjogNywgImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOiAwLjUsICJiZWFyaW5nIjogNTAsICJhbHRpdHVkZSI6IDIwLCAic3BlZWQiOiAxNn19XX0=", "se_ca": "my_category", "p": "pc"}, {"dtm": "1410184746895", "e": "se", "eid": "83eac227-7129-42a7-af45-40f63ed4b9dd", "se_ac": "another_action", "tv": "py-0.5.0", "cx": "eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY29udGV4dHMvanNvbnNjaGVtYS8xLTAtMCIsICJkYXRhIjogW3sic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L21vYmlsZV9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IHsiZGV2aWNlTWFudWZhY3R1cmVyIjogIkFtc3RyYWQiLCAiYW5kcm9pZElkZmEiOiAic29tZV9hbmRyb2lkSWRmYSIsICJkZXZpY2VNb2RlbCI6ICJsYXJnZSIsICJvcGVuSWRmYSI6ICJzb21lX0lkZmEiLCAiY2FycmllciI6ICJzb21lX2NhcnJpZXIiLCAiYXBwbGVJZGZhIjogInNvbWVfYXBwbGVJZGZhIiwgIm9zVmVyc2lvbiI6ICIzLjAuMCIsICJhcHBsZUlkZnYiOiAic29tZV9hcHBsZUlkZnYiLCAib3NUeXBlIjogIk9TWCJ9fSwgeyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvZ2VvbG9jYXRpb25fY29udGV4dC9qc29uc2NoZW1hLzEtMC0wIiwgImRhdGEiOiB7ImxvbmdpdHVkZSI6IDEwLCAiYWx0aXR1ZGVBY2N1cmFjeSI6IDAuMywgImxhdGl0dWRlIjogNywgImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOiAwLjUsICJiZWFyaW5nIjogNTAsICJhbHRpdHVkZSI6IDIwLCAic3BlZWQiOiAxNn19XX0=", "se_ca": "another_category", "p": "pc"}]}""".some ! DateTime
        .parse("2014-09-08T13:59:07.000+00:00") !
        "python-requests%2F2.2.1+CPython%2F3.3.5+Linux%2F3.2.0-61-generic".some ! None |
      "CallRail-style POST w/o body, content-type" !! "2013-08-29  00:18:48  -  830 37.157.33.123 POST d3v6ndkyapxc2w.cloudfront.net /i  200 http://snowplowanalytics.com/analytics/index.html Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0 e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html - - -" !
        "tp1" ! toNameValuePairs(
        "e" -> "pv",
        "page" -> "Introduction - Snowplow Analytics%",
        "dtm" -> "1377735557970",
        "tid" -> "567074",
        "vp" -> "1024x635",
        "ds" -> "1024x635",
        "vid" -> "1",
        "duid" -> "7969620089de36eb",
        "p" -> "web",
        "tv" -> "js-0.12.0",
        "fp" -> "308909339",
        "aid" -> "snowplowweb",
        "lang" -> "en-US",
        "cs" -> "UTF-8",
        "tz" -> "America/Los_Angeles",
        "refr" -> "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
        "f_pdf" -> "1",
        "f_qt" -> "1",
        "f_realp" -> "0",
        "f_wma" -> "1",
        "f_dir" -> "0",
        "f_fla" -> "1",
        "f_java" -> "1",
        "f_gears" -> "0",
        "f_ag" -> "0",
        "res" -> "1024x768",
        "cd" -> "24",
        "cookie" -> "1",
        "url" -> "http://snowplowanalytics.com/analytics/index.html"
      ) ! None ! None ! DateTime.parse("2013-08-29T00:18:48.000+00:00") !
        "Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0".some ! "http://snowplowanalytics.com/analytics/index.html".some |
      // This may not be a valid GET but we need to accept it because Lumia emits it (#2743, #489)
      "Snowplow Tp1 GET w/ content-type no body " !! "2013-08-29  00:18:48  -  830 37.157.33.123 GET d3v6ndkyapxc2w.cloudfront.net /i  200 http://snowplowanalytics.com/analytics/index.html Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0 e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html - - - application%2Fx-www-form-urlencoded%3B+charset%3Dutf-8 -" !
        "tp1" ! toNameValuePairs(
        "e" -> "pv",
        "page" -> "Introduction - Snowplow Analytics%",
        "dtm" -> "1377735557970",
        "tid" -> "567074",
        "vp" -> "1024x635",
        "ds" -> "1024x635",
        "vid" -> "1",
        "duid" -> "7969620089de36eb",
        "p" -> "web",
        "tv" -> "js-0.12.0",
        "fp" -> "308909339",
        "aid" -> "snowplowweb",
        "lang" -> "en-US",
        "cs" -> "UTF-8",
        "tz" -> "America/Los_Angeles",
        "refr" -> "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
        "f_pdf" -> "1",
        "f_qt" -> "1",
        "f_realp" -> "0",
        "f_wma" -> "1",
        "f_dir" -> "0",
        "f_fla" -> "1",
        "f_java" -> "1",
        "f_gears" -> "0",
        "f_ag" -> "0",
        "res" -> "1024x768",
        "cd" -> "24",
        "cookie" -> "1",
        "url" -> "http://snowplowanalytics.com/analytics/index.html"
      ) ! "application/x-www-form-urlencoded; charset=utf-8".some ! None ! DateTime.parse(
        "2013-08-29T00:18:48.000+00:00"
      ) !
        "Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0".some ! "http://snowplowanalytics.com/analytics/index.html".some |> {

      (_, raw, version, payload, contentType, body, timestamp, userAgent, refererUri) =>
        {

          val canonicalEvent = CljTomcatLoader.toCollectorPayload(raw, Process)

          val expected = CollectorPayload(
            api = CollectorPayload.Api(Expected.vendor, version),
            querystring = payload,
            contentType = contentType,
            body = body,
            source = CollectorPayload.Source(Expected.collector, Expected.encoding, None),
            context = CollectorPayload
              .Context(timestamp.some, Expected.ipAddress, userAgent, refererUri, Nil, None)
          )

          canonicalEvent must beValid(expected.some)
        }
    }

  def e2 = {
    val raw =
      "2014-09-08 13:59:07  - - 37.157.33.123 GET  - /com.snowplowanalytics.snowplow/tp2 200 - python-requests%2F2.2.1+CPython%2F3.3.5+Linux%2F3.2.0-61-generic  &cv=clj-0.7.0-tom-0.1.0&nuid=5c6c40e4-eff8-409b-9327-471f303e30b6 - - - application%2Fjson%3B+charset%3Dutf-8 eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvcGF5bG9hZF9kYXRhL2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IFt7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAicHYiLCAiZWlkIjogIjJjYWU0MTkxLTMxY2QtNDc4My04MmE4LWRmNTMxOGY0NGFmZiIsICJ1cmwiOiAiaHR0cDovL3d3dy5leGFtcGxlLmNvbSIsICJ0diI6ICJweS0wLjUuMCIsICJjeCI6ICJleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WTI5dWRHVjRkSE12YW5OdmJuTmphR1Z0WVM4eExUQXRNQ0lzSUNKa1lYUmhJam9nVzNzaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyMXZZbWxzWlY5amIyNTBaWGgwTDJwemIyNXpZMmhsYldFdk1TMHdMVEFpTENBaVpHRjBZU0k2SUhzaVpHVjJhV05sVFdGdWRXWmhZM1IxY21WeUlqb2dJa0Z0YzNSeVlXUWlMQ0FpWVc1a2NtOXBaRWxrWm1FaU9pQWljMjl0WlY5aGJtUnliMmxrU1dSbVlTSXNJQ0prWlhacFkyVk5iMlJsYkNJNklDSnNZWEpuWlNJc0lDSnZjR1Z1U1dSbVlTSTZJQ0p6YjIxbFgwbGtabUVpTENBaVkyRnljbWxsY2lJNklDSnpiMjFsWDJOaGNuSnBaWElpTENBaVlYQndiR1ZKWkdaaElqb2dJbk52YldWZllYQndiR1ZKWkdaaElpd2dJbTl6Vm1WeWMybHZiaUk2SUNJekxqQXVNQ0lzSUNKaGNIQnNaVWxrWm5ZaU9pQWljMjl0WlY5aGNIQnNaVWxrWm5ZaUxDQWliM05VZVhCbElqb2dJazlUV0NKOWZTd2dleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WjJWdmJHOWpZWFJwYjI1ZlkyOXVkR1Y0ZEM5cWMyOXVjMk5vWlcxaEx6RXRNQzB3SWl3Z0ltUmhkR0VpT2lCN0lteHZibWRwZEhWa1pTSTZJREV3TENBaVlXeDBhWFIxWkdWQlkyTjFjbUZqZVNJNklEQXVNeXdnSW14aGRHbDBkV1JsSWpvZ055d2dJbXhoZEdsMGRXUmxURzl1WjJsMGRXUmxRV05qZFhKaFkza2lPaUF3TGpVc0lDSmlaV0Z5YVc1bklqb2dOVEFzSUNKaGJIUnBkSFZrWlNJNklESXdMQ0FpYzNCbFpXUWlPaUF4Tm4xOVhYMD0iLCAicCI6ICJwYyJ9LCB7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAic2UiLCAiZWlkIjogIjVhNzExODg1LTY5ZGMtNGY0Mi04Nzg1LWZjNjVmMTc1OGVjMCIsICJzZV9hYyI6ICJteV9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogIm15X2NhdGVnb3J5IiwgInAiOiAicGMifSwgeyJkdG0iOiAiMTQxMDE4NDc0Njg5NSIsICJlIjogInNlIiwgImVpZCI6ICI4M2VhYzIyNy03MTI5LTQyYTctYWY0NS00MGY2M2VkNGI5ZGQiLCAic2VfYWMiOiAiYW5vdGhlcl9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogImFub3RoZXJfY2F0ZWdvcnkiLCAicCI6ICJwYyJ9XX0"
    val actual = CljTomcatLoader.toCollectorPayload(raw, Process)
    actual must beInvalid.like {
      case NonEmptyList(
          BadRow.CPFormatViolation(
            Process,
            Failure.CPFormatViolation(_, "clj-tomcat", f),
            Payload.RawPayload(l)
          ),
          List()
          ) =>
        f must_== FailureDetails.CPFormatViolationMessage.InputData(
          "verb",
          "GET".some,
          "operation must be POST if content type and/or body are provided"
        )
        l must_== raw
    }
  }

  def e3 = {
    val raw =
      "2014-09-08 13:59:07  - - 37.157.33.123 POST  - /com.sendgrid-api-v3 200 - python-requests%2F2.2.1+CPython%2F3.3.5+Linux%2F3.2.0-61-generic  &cv=clj-0.7.0-tom-0.1.0&nuid=5c6c40e4-eff8-409b-9327-471f303e30b6 - - - application%2Fjson%3B+charset%3Dutf-8 eyJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvcGF5bG9hZF9kYXRhL2pzb25zY2hlbWEvMS0wLTAiLCAiZGF0YSI6IFt7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAicHYiLCAiZWlkIjogIjJjYWU0MTkxLTMxY2QtNDc4My04MmE4LWRmNTMxOGY0NGFmZiIsICJ1cmwiOiAiaHR0cDovL3d3dy5leGFtcGxlLmNvbSIsICJ0diI6ICJweS0wLjUuMCIsICJjeCI6ICJleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WTI5dWRHVjRkSE12YW5OdmJuTmphR1Z0WVM4eExUQXRNQ0lzSUNKa1lYUmhJam9nVzNzaWMyTm9aVzFoSWpvZ0ltbG5iSFU2WTI5dExuTnViM2R3Ykc5M1lXNWhiSGwwYVdOekxuTnViM2R3Ykc5M0wyMXZZbWxzWlY5amIyNTBaWGgwTDJwemIyNXpZMmhsYldFdk1TMHdMVEFpTENBaVpHRjBZU0k2SUhzaVpHVjJhV05sVFdGdWRXWmhZM1IxY21WeUlqb2dJa0Z0YzNSeVlXUWlMQ0FpWVc1a2NtOXBaRWxrWm1FaU9pQWljMjl0WlY5aGJtUnliMmxrU1dSbVlTSXNJQ0prWlhacFkyVk5iMlJsYkNJNklDSnNZWEpuWlNJc0lDSnZjR1Z1U1dSbVlTSTZJQ0p6YjIxbFgwbGtabUVpTENBaVkyRnljbWxsY2lJNklDSnpiMjFsWDJOaGNuSnBaWElpTENBaVlYQndiR1ZKWkdaaElqb2dJbk52YldWZllYQndiR1ZKWkdaaElpd2dJbTl6Vm1WeWMybHZiaUk2SUNJekxqQXVNQ0lzSUNKaGNIQnNaVWxrWm5ZaU9pQWljMjl0WlY5aGNIQnNaVWxrWm5ZaUxDQWliM05VZVhCbElqb2dJazlUV0NKOWZTd2dleUp6WTJobGJXRWlPaUFpYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2WjJWdmJHOWpZWFJwYjI1ZlkyOXVkR1Y0ZEM5cWMyOXVjMk5vWlcxaEx6RXRNQzB3SWl3Z0ltUmhkR0VpT2lCN0lteHZibWRwZEhWa1pTSTZJREV3TENBaVlXeDBhWFIxWkdWQlkyTjFjbUZqZVNJNklEQXVNeXdnSW14aGRHbDBkV1JsSWpvZ055d2dJbXhoZEdsMGRXUmxURzl1WjJsMGRXUmxRV05qZFhKaFkza2lPaUF3TGpVc0lDSmlaV0Z5YVc1bklqb2dOVEFzSUNKaGJIUnBkSFZrWlNJNklESXdMQ0FpYzNCbFpXUWlPaUF4Tm4xOVhYMD0iLCAicCI6ICJwYyJ9LCB7ImR0bSI6ICIxNDEwMTg0NzQ2ODk0IiwgImUiOiAic2UiLCAiZWlkIjogIjVhNzExODg1LTY5ZGMtNGY0Mi04Nzg1LWZjNjVmMTc1OGVjMCIsICJzZV9hYyI6ICJteV9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogIm15X2NhdGVnb3J5IiwgInAiOiAicGMifSwgeyJkdG0iOiAiMTQxMDE4NDc0Njg5NSIsICJlIjogInNlIiwgImVpZCI6ICI4M2VhYzIyNy03MTI5LTQyYTctYWY0NS00MGY2M2VkNGI5ZGQiLCAic2VfYWMiOiAiYW5vdGhlcl9hY3Rpb24iLCAidHYiOiAicHktMC41LjAiLCAiY3giOiAiZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdlkyOXVkR1Y0ZEhNdmFuTnZibk5qYUdWdFlTOHhMVEF0TUNJc0lDSmtZWFJoSWpvZ1czc2ljMk5vWlcxaElqb2dJbWxuYkhVNlkyOXRMbk51YjNkd2JHOTNZVzVoYkhsMGFXTnpMbk51YjNkd2JHOTNMMjF2WW1sc1pWOWpiMjUwWlhoMEwycHpiMjV6WTJobGJXRXZNUzB3TFRBaUxDQWlaR0YwWVNJNklIc2laR1YyYVdObFRXRnVkV1poWTNSMWNtVnlJam9nSWtGdGMzUnlZV1FpTENBaVlXNWtjbTlwWkVsa1ptRWlPaUFpYzI5dFpWOWhibVJ5YjJsa1NXUm1ZU0lzSUNKa1pYWnBZMlZOYjJSbGJDSTZJQ0pzWVhKblpTSXNJQ0p2Y0dWdVNXUm1ZU0k2SUNKemIyMWxYMGxrWm1FaUxDQWlZMkZ5Y21sbGNpSTZJQ0p6YjIxbFgyTmhjbkpwWlhJaUxDQWlZWEJ3YkdWSlpHWmhJam9nSW5OdmJXVmZZWEJ3YkdWSlpHWmhJaXdnSW05elZtVnljMmx2YmlJNklDSXpMakF1TUNJc0lDSmhjSEJzWlVsa1puWWlPaUFpYzI5dFpWOWhjSEJzWlVsa1puWWlMQ0FpYjNOVWVYQmxJam9nSWs5VFdDSjlmU3dnZXlKelkyaGxiV0VpT2lBaWFXZHNkVHBqYjIwdWMyNXZkM0JzYjNkaGJtRnNlWFJwWTNNdWMyNXZkM0JzYjNjdloyVnZiRzlqWVhScGIyNWZZMjl1ZEdWNGRDOXFjMjl1YzJOb1pXMWhMekV0TUMwd0lpd2dJbVJoZEdFaU9pQjdJbXh2Ym1kcGRIVmtaU0k2SURFd0xDQWlZV3gwYVhSMVpHVkJZMk4xY21GamVTSTZJREF1TXl3Z0lteGhkR2wwZFdSbElqb2dOeXdnSW14aGRHbDBkV1JsVEc5dVoybDBkV1JsUVdOamRYSmhZM2tpT2lBd0xqVXNJQ0ppWldGeWFXNW5Jam9nTlRBc0lDSmhiSFJwZEhWa1pTSTZJREl3TENBaWMzQmxaV1FpT2lBeE5uMTlYWDA9IiwgInNlX2NhIjogImFub3RoZXJfY2F0ZWdvcnkiLCAicCI6ICJwYyJ9XX0"
    val actual = CljTomcatLoader.toCollectorPayload(raw, Process)
    actual must beInvalid.like {
      case NonEmptyList(
          BadRow.CPFormatViolation(
            Process,
            Failure.CPFormatViolation(_, "clj-tomcat", f),
            Payload.RawPayload(l)
          ),
          List()
          ) =>
        f must_== FailureDetails.CPFormatViolationMessage.InputData(
          "path",
          "/com.sendgrid-api-v3".some,
          "path does not match (/)vendor/version(/) nor is a legacy /i(ce.png) request"
        )
        l must_== raw
    }
  }

  // A bit of fun: the chances of generating a valid Clojure Collector log record at random are
  // so low that we can just use ScalaCheck here
  def e4 =
    prop { (raw: String) =>
      CljTomcatLoader.toCollectorPayload(raw, Process) must beInvalid.like {
        case NonEmptyList(
            BadRow.CPFormatViolation(
              Process,
              Failure.CPFormatViolation(_, "clj-tomcat", f),
              Payload.RawPayload(l)
            ),
            List()
            ) =>
          f must_== FailureDetails.CPFormatViolationMessage.Fallback(
            "does not match the raw event format"
          )
          l must_== raw
      }
    }
}
