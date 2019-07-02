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

object UnbounceAdapterSpec {
  val body =
    "page_url=http%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F&page_name=Wayfaring&page_id=7648177d-7323-4330-b4f9-9951a52138b6&variant=a&data.json=%7B%22userfield1%22%3A%5B%22asdfasdfad%22%5D%2C%22ip_address%22%3A%5B%2285.73.39.163%22%5D%2C%22page_uuid%22%3A%5B%227648177d-7323-4330-b4f9-9951a52138b6%22%5D%2C%22variant%22%3A%5B%22a%22%5D%2C%22time_submitted%22%3A%5B%2212%3A12+PM+UTC%22%5D%2C%22date_submitted%22%3A%5B%222017-11-15%22%5D%2C%22page_url%22%3A%5B%22http%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F%22%5D%2C%22page_name%22%3A%5B%22Wayfaring%22%5D%7D&data.xml=%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22%3F%3E%3Cform_data%3E%3Cuserfield1%3Easdfasdfad%3C%2Fuserfield1%3E%3Cip_address%3E85.73.39.163%3C%2Fip_address%3E%3Cpage_uuid%3E7648177d-7323-4330-b4f9-9951a52138b6%3C%2Fpage_uuid%3E%3Cvariant%3Ea%3C%2Fvariant%3E%3Ctime_submitted%3E12%3A12+PM+UTC%3C%2Ftime_submitted%3E%3Cdate_submitted%3E2017-11-15%3C%2Fdate_submitted%3E%3Cpage_url%3Ehttp%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F%3C%2Fpage_url%3E%3Cpage_name%3EWayfaring%3C%2Fpage_name%3E%3C%2Fform_data%3E"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.unbounce/v1",
      body = body.some,
      contentType = "application/x-www-form-urlencoded".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.unbounce-v1",
    "event_vendor" -> "com.unbounce",
    "event_name" -> "form_post",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.unbounce/form_post/jsonschema/1-0-0","data":{"data.json":{"userfield1":["asdfasdfad"],"ipAddress":["85.73.39.163"],"pageUuid":["7648177d-7323-4330-b4f9-9951a52138b6"],"variant":["a"],"timeSubmitted":["12:12 PM UTC"],"dateSubmitted":["2017-11-15"],"pageUrl":["http://unbouncepages.com/wayfaring-147/"],"pageName":["Wayfaring"]},"variant":"a","pageId":"7648177d-7323-4330-b4f9-9951a52138b6","pageName":"Wayfaring","pageUrl":"http://unbouncepages.com/wayfaring-147/"}}}""".noSpaces
  )
}

class UnbounceAdapterSpec extends PipelineSpec {
  import UnbounceAdapterSpec._
  "UnbounceAdapter" should "enrich using the unbounce adapter" in {
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
