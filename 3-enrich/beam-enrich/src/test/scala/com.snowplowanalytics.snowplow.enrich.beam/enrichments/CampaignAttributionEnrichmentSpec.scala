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

object CampaignAttributionEnrichmentSpec {
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      refererUri =
        "http://pb.com/?utm_source=GoogleSearch&utm_medium=cpc&utm_term=pb&utm_content=39&cid=tna&gclid=CI6".some,
      path = "/i",
      querystring = "e=pp".some
    )
  )
  val expected = Map(
    "mkt_content" -> "39",
    "mkt_clickid" -> "CI6",
    "mkt_term" -> "pb",
    "mkt_campaign" -> "tna",
    "mkt_source" -> "GoogleSearch",
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "page_ping",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "page_ping"
  )
}

class CampaignAttributionEnrichmentSpec extends PipelineSpec {
  import CampaignAttributionEnrichmentSpec._
  "CampaignAttributionEnrichment" should "enrich using the campaign attribution enrichment" in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/campaign_attribution").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(DistCacheIO(""), List.empty[Either[String, String]])
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          // once scio accepts Assertion
          //SpecHelpers.buildEnrichedEvent(c) should contain allElementsOf (expected)
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
