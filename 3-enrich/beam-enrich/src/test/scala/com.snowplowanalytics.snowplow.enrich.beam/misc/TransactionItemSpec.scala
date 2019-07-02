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

object TransactionItemSpec {
  val querystring =
    "e=ti&ti_id=order-123&ti_sk=PBZ1001&ti_na=Blue%20t-shirt&ti_ca=APPAREL&ti_pr=2000&ti_qu=2"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/ice.png",
      querystring = querystring.some
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "transaction_item",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "transaction_item",
    "ti_orderid" -> "order-123",
    "ti_sku" -> "PBZ1001",
    "ti_quantity" -> "2",
    "ti_currency" -> "",
    "ti_category" -> "APPAREL",
    "ti_price" -> "2000",
    "ti_price_base" -> ""
  )
}

class TransactionItemSpec extends PipelineSpec {
  import TransactionItemSpec._
  "Enrich" should "enrich and produce a transaction item" in {
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
          println(SpecHelpers.buildEnrichedEvent(c).filter { case (k, _) => expected.contains(k) })
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
