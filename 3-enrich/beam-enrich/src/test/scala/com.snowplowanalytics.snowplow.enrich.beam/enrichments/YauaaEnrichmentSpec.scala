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

object YauaaEnrichmentSpec {
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "page_ping",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "page_ping",
    "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Desktop","layoutEngineNameVersion":"Gecko 12.0","operatingSystemNameVersion":"Windows 7","layoutEngineBuild":"20100101","layoutEngineNameVersionMajor":"Gecko 12","operatingSystemName":"Windows NT","agentVersionMajor":"12","layoutEngineVersionMajor":"12","deviceClass":"Desktop","agentNameVersionMajor":"Firefox 12","deviceCpuBits":"64","operatingSystemClass":"Desktop","layoutEngineName":"Gecko","agentName":"Firefox","agentVersion":"12.0","layoutEngineClass":"Browser","agentNameVersion":"Firefox 12.0","operatingSystemVersion":"7","deviceCpu":"Intel x86_64","agentClass":"Browser","layoutEngineVersion":"12.0"}}]}""".noSpaces
  )
}

class YauaaEnrichmentSpec extends PipelineSpec {
  import YauaaEnrichmentSpec._
  "YauaaEnrichment" should "enrich using the yauaa enrichment" in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/yauaa").toURI())
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
