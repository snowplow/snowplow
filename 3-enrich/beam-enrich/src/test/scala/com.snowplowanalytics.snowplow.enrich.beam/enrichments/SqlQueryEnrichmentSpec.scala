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
package enrichments

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._
import io.circe.literal._

object SqlQueryEnrichmentSpec {
  val contexts =
    """eyJkYXRhIjpbeyJkYXRhIjp7ImxvbmdpdHVkZSI6MTAsImJlYXJpbmciOjUwLCJzcGVlZCI6MjUuMCwiYWx0aXR1ZGUiOjIwLCJhbHRpdHVkZUFjY3VyYWN5IjowLjMsImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOjAuNSwibGF0aXR1ZGUiOjd9LCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9nZW9sb2NhdGlvbl9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAifV0sInNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L2NvbnRleHRzL2pzb25zY2hlbWEvMS0wLTAifQo="""
  val unstructEvent =
    """%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow%2Funstruct_event%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow-website%2Fsignup_form_submitted%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22name%22%3A%22Bob%C2%AE%22%2C%22email%22%3A%22alex%2Btest%40snowplowanalytics.com%22%2C%22company%22%3A%22SP%22%2C%22eventsPerMonth%22%3A%22%3C%201%20million%22%2C%22serviceType%22%3A%22unsure%22%7D%7D%7D"""
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      querystring = s"e=ue&cx=$contexts&ue_pr=$unstructEvent".some,
      path = "/i",
      userAgent = Some("Milton")
    )
  )
  val expected = Map(
    "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/user/jsonschema/1-0-0","data":[{"city":"Milton","country":"US","pk":4}]}]}""".noSpaces
  )
}

class SqlQueryEnrichmentSpec extends PipelineSpec {
  import SqlQueryEnrichmentSpec._
  "SqlQueryEnrichment" should "enrich using the sql query enrichment" taggedAs CI in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/sql_query").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(DistCacheIO(""), List.empty[Either[String, String]])
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .run()
  }
}
