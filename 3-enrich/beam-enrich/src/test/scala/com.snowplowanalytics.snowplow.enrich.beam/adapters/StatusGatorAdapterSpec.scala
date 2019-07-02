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

object StatusGatorAdapterSpec {
  val body =
    "service_name=Amazon+Web+Services&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Famazon-web-services.ico&status_page_url=http%3A%2F%2Fstatus.aws.amazon.com%2F&home_page_url=http%3A%2F%2Faws.amazon.com%2F&current_status=warn&last_status=up&occurred_at=2017-11-11T15%3A36%3A18%2B00%3A00"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.statusgator/v1",
      body = body.some,
      contentType = "application/x-www-form-urlencoded".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.statusgator-v1",
    "event_vendor" -> "com.statusgator",
    "event_name" -> "status_change",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0","data":{"lastStatus":"up","statusPageUrl":"http://status.aws.amazon.com/","serviceName":"Amazon Web Services","faviconUrl":"https://dwxjd9cd6rwno.cloudfront.net/favicons/amazon-web-services.ico","occurredAt":"2017-11-11T15:36:18+00:00","homePageUrl":"http://aws.amazon.com/","currentStatus":"warn"}}}""".noSpaces
  )
}

class StatusGatorAdapterSpec extends PipelineSpec {
  import StatusGatorAdapterSpec._
  "StatusGatorAdapter" should "enrich using the status gator adapter" in {
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
