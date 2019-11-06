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

object CurrencyConversionEnrichmentTransactionSpec {
  val querystring =
    "e=tr&tr_id=order-123&tr_af=pb&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&tr_cu=USD&tid=028288"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/ice.png",
      querystring = querystring.some,
      timestamp = 1562008983000L
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "transaction",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "transaction",
    "base_currency" -> "EUR",
    "tr_currency" -> "USD",
    "tr_affiliation" -> "pb",
    "tr_total" -> "8000",
    "tr_total_base" -> "7087.49",
    "tr_tax" -> "200",
    "tr_tax_base" -> "177.19",
    "tr_shipping" -> "50",
    "tr_shipping_base" -> "44.30",
    "tr_orderid" -> "order-123",
    "tr_state" -> "England",
    "txn_id" -> "028288",
    "tr_country" -> "UK",
    "tr_city" -> "London",
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

class CurrencyConversionEnrichmentTransactionSpec extends PipelineSpec {
  import CurrencyConversionEnrichmentTransactionSpec._
  "CurrencyConversionEnrichment" should "enrich and convert currencies for a transaction" taggedAs OER in {

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
