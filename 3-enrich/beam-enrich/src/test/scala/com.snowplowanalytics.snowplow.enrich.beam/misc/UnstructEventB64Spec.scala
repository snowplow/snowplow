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

object UnstructEventB64Spec {
  val querystring =
    "e=ue&ue_px=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy5pbnB1dC1hZGFwdGVycy9zZWdtZW50X3dlYmhvb2tfY29uZmlnL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7InZlbmRvciI6Is6nzrHPgc65z4TOr869zrcgTkVXIFVuaWNvZGUgdGVzdCIsIm5hbWUiOiJhbGV4K3Rlc3RAc25vd3Bsb3dhbmFseXRpY3MuY29tIiwicGFyYW1ldGVycyI6eyJtYXBwaW5ncyI6eyJldmVudHNQZXJNb250aCI6IjwgMSBtaWxsaW9uIiwic2VydmljZVR5cGUiOiJ1bnN1cmUifX19fX0="
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/ice.png",
      querystring = querystring.some
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow.input-adapters",
    "event_name" -> "segment_webhook_config",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow.input-adapters/segment_webhook_config/jsonschema/1-0-0","data":{"vendor":"Χαριτίνη NEW Unicode test","name":"alex+test@snowplowanalytics.com","parameters":{"mappings":{"eventsPerMonth":"< 1 million","serviceType":"unsure"}}}}}""".noSpaces
  )
}

class UnstructEventB64Spec extends PipelineSpec {
  import UnstructEventB64Spec._
  "Enrich" should "enrich and produce an unstruct event if it was b64" in {
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
