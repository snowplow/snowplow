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

import java.nio.file.{Files, Path, Paths}

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._

object CurrencyConversionEnrichmentTransactionItemSpec {
  val querystring =
    "e=ti&ti_id=order-123&ti_sk=PBZ1001&ti_na=Blue%20t-shirt&ti_ca=APPAREL&ti_pr=2000&ti_qu=2&ti_cu=GBP&tid=851830"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/ice.png",
      querystring = querystring.some,
      timestamp = 1562008983000L
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "transaction_item",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "transaction_item",
    "base_currency" -> "EUR",
    "ti_currency" -> "GBP",
    "ti_orderid" -> "order-123",
    "ti_sku" -> "PBZ1001",
    "ti_quantity" -> "2",
    "ti_category" -> "APPAREL",
    "ti_price" -> "2000",
    "ti_price_base" -> "2240.45",
    "ti_name" -> "Blue t-shirt",
    "collector_tstamp" -> "2019-07-01 19:23:03.000"
  )
  val config = s"""{
    "schema": "iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0",
    "data": {
      "enabled": true,
      "vendor": "com.snowplowanalytics.snowplow",
      "name": "currency_conversion_config",
      "parameters": {
        "accountType": "DEVELOPER",
        "apiKey": "${sys.env.get("OER_KEY").getOrElse("-")}",
        "baseCurrency": "EUR",
        "rateAt": "EOD_PRIOR"
      }
    }
  }"""

}

class CurrencyConversionEnrichmentTransactionItemSpec extends PipelineSpec {
  import CurrencyConversionEnrichmentTransactionItemSpec._
  "CurrencyConversionEnrichment" should "enrich and convert currencies for a transaction item" taggedAs OER in {

    val enrichmentPath: Path = Files.createTempDirectory("currency-conversion")
    SpecHelpers.write(Paths.get(enrichmentPath.toString, "currency-conversion.json"), config)

    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + enrichmentPath
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
