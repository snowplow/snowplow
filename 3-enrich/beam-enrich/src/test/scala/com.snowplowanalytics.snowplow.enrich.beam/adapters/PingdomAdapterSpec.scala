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

object PingdomAdapterSpec {
  val querystring =
    "p=srv&message=%7B%22check%22%3A%20%221421338%22%2C%20%22checkname%22%3A%20%22Webhooks_Test%22%2C%20%22host%22%3A%20%227eef51c2.ngrok.com%22%2C%20%22action%22%3A%20%22assign%22%2C%20%22incidentid%22%3A%203%2C%20%22description%22%3A%20%22down%22%7D"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.pingdom/v1",
      querystring = querystring.some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.pingdom-v1",
    "event_vendor" -> "com.pingdom",
    "event_name" -> "incident_assign",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}""".noSpaces
  )
}

class PingdomAdapterSpec extends PipelineSpec {
  import PingdomAdapterSpec._
  "PingdomAdapter" should "enrich using the pingdom adapter" in {
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
