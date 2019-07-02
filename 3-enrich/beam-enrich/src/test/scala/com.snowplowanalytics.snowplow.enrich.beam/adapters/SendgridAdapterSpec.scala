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

object SendgridAdapterSpec {
  val body =
    json"""[{"email":"example@test.com","timestamp":1446549615,"smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","event":"processed","category":"cat facts","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0","marketing_campaign_id":12345,"marketing_campaign_name":"campaign name","marketing_campaign_version":"B","marketing_campaign_split_id":13471}]"""
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.sendgrid/v3",
      body = body.noSpaces.some,
      contentType = "application/json".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.sendgrid-v3",
    "event_vendor" -> "com.sendgrid",
    "event_name" -> "processed",
    "event_format" -> "jsonschema",
    "event_version" -> "2-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/processed/jsonschema/2-0-0","data":{"timestamp":"2015-11-03T11:20:15.000Z","email":"example@test.com","marketing_campaign_name":"campaign name","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>","marketing_campaign_version":"B","marketing_campaign_id":12345,"marketing_campaign_split_id":13471,"category":"cat facts","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}""".noSpaces
  )
}

class SendgridAdapterSpec extends PipelineSpec {
  import SendgridAdapterSpec._
  "SendgridAdapter" should "enrich using the sendgrid adapter" in {
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
