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

object IabEnrichmentSpec {
  // nuids are here to maintain an order
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      userAgent =
        "Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0".some,
      ipAddress = "216.160.83.56",
      networkUserId = "11111111-1111-1111-1111-111111111111"
    ),
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      userAgent =
        "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Safari/537.36".some,
      ipAddress = "216.160.83.56",
      networkUserId = "22222222-2222-2222-2222-222222222222"
    ),
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      userAgent =
        "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Safari/537.36".some,
      ipAddress = "216.160.83.56:8080",
      networkUserId = "33333333-3333-3333-3333-333333333333"
    ),
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      userAgent =
        "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Safari/537.36".some,
      ipAddress = "2001:db8:0:0:0:ff00:42:8329",
      networkUserId = "44444444-4444-4444-4444-444444444444"
    ),
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring = "e=pp".some,
      ipAddress = "216.160.83.56",
      networkUserId = "55555555-5555-5555-5555-555555555555"
    )
  )
  val expecteds = List(
    Map(
      "event_vendor" -> "com.snowplowanalytics.snowplow",
      "event_name" -> "page_ping",
      "event_format" -> "jsonschema",
      "event_version" -> "1-0-0",
      "event" -> "page_ping",
      "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0","data":{"spiderOrRobot":false,"category":"BROWSER","reason":"PASSED_ALL","primaryImpact":"NONE"}}]}""".noSpaces
    ),
    Map(
      "event_vendor" -> "com.snowplowanalytics.snowplow",
      "event_name" -> "page_ping",
      "event_format" -> "jsonschema",
      "event_version" -> "1-0-0",
      "event" -> "page_ping",
      "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0","data":{"spiderOrRobot":true,"category":"SPIDER_OR_ROBOT","reason":"FAILED_UA_INCLUDE","primaryImpact":"UNKNOWN"}}]}""".noSpaces
    ),
    Map(
      "event_vendor" -> "com.snowplowanalytics.snowplow",
      "event_name" -> "page_ping",
      "event_format" -> "jsonschema",
      "event_version" -> "1-0-0",
      "event" -> "page_ping",
      "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0","data":{"spiderOrRobot":true,"category":"SPIDER_OR_ROBOT","reason":"FAILED_UA_INCLUDE","primaryImpact":"UNKNOWN"}}]}""".noSpaces
    ),
    Map(
      "event_vendor" -> "com.snowplowanalytics.snowplow",
      "event_name" -> "page_ping",
      "event_format" -> "jsonschema",
      "event_version" -> "1-0-0",
      "event" -> "page_ping",
      "derived_contexts" -> ""
    ),
    Map(
      "event_vendor" -> "com.snowplowanalytics.snowplow",
      "event_name" -> "page_ping",
      "event_format" -> "jsonschema",
      "event_version" -> "1-0-0",
      "event" -> "page_ping",
      "derived_contexts" -> ""
    )
  )
}

class IabEnrichmentSpec extends PipelineSpec {
  import IabEnrichmentSpec._
  "IabEnrichment" should "enrich using the iab enrichment" in {
    val localIpFile = "./iab_ipFile"
    val resourceIpFile = "/iab/ip_exclude_current_cidr.txt"
    val localExcludeUaFile = "./iab_excludeUseragentFile"
    val resourceExcludeUaFile = "/iab/exclude_current.txt"
    val localIncludeUaFile = "./iab_includeUseragentFile"
    val resourceIncludeUaFile = "/iab/include_current.txt"
    SpecHelpers.copyResource(resourceIpFile, localIpFile)
    SpecHelpers.copyResource(resourceExcludeUaFile, localExcludeUaFile)
    SpecHelpers.copyResource(resourceIncludeUaFile, localIncludeUaFile)

    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/iab").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(
        DistCacheIO(
          Seq(
            s"http://snowplow-hosted-assets.s3.amazonaws.com/third-party$resourceIpFile",
            s"http://snowplow-hosted-assets.s3.amazonaws.com/third-party$resourceExcludeUaFile",
            s"http://snowplow-hosted-assets.s3.amazonaws.com/third-party$resourceIncludeUaFile"
          )
        ),
        List(Right(localIpFile), Right(localExcludeUaFile), Right(localIncludeUaFile))
      )
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO[String]("out")) { o =>
        o should satisfy { cs: Iterable[String] =>
          val ordered =
            cs.toList
              .map(SpecHelpers.buildEnrichedEvent)
              .sortWith(
                _.get("network_userid").getOrElse("") < _.get("network_userid").getOrElse("")
              )
          ordered.zip(expecteds).forall { case (c, e) => SpecHelpers.compareEnrichedEvent(e, c) }
        }; ()
      }
      .run()

    SpecHelpers.deleteLocalFile(localIpFile)
    SpecHelpers.deleteLocalFile(localExcludeUaFile)
    SpecHelpers.deleteLocalFile(localIncludeUaFile)
  }
}
