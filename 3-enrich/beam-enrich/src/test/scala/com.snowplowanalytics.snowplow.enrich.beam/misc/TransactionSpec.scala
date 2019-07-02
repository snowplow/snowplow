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
package misc

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._
import io.circe.literal._

object TransactionSpec {
  val querystring =
    "e=tr&tr_id=order-123&tr_af=pb&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&cx=eyJkYXRhIjpbeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91cmlfcmVkaXJlY3QvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsidXJpIjoiaHR0cDovL3Nub3dwbG93YW5hbHl0aWNzLmNvbS8ifX1dLCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIn0=&tid=028288"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/ice.png",
      querystring = querystring.some
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "transaction",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "transaction",
    "tr_affiliation" -> "pb",
    "tr_total" -> "8000",
    "tr_total_base" -> "",
    "tr_tax" -> "200",
    "tr_tax_base" -> "",
    "tr_shipping" -> "50",
    "tr_shipping_base" -> "",
    "tr_orderid" -> "order-123",
    "tr_state" -> "England",
    "txn_id" -> "028288",
    "tr_country" -> "UK",
    "tr_city" -> "London",
    "contexts" -> json"""{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"http://snowplowanalytics.com/"}}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""".noSpaces
  )
}

class TransactionSpec extends PipelineSpec {
  import TransactionSpec._
  "Enrich" should "enrich and produce a transaction" in {
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
