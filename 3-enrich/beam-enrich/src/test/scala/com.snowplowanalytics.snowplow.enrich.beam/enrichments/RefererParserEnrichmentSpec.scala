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

object RefererParserEnrichmentSpec {
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/i",
      querystring =
        "e=pp&refr=http%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3D%250Agateway%2509oracle%2509cards%2509denise%2509linn%26hl%3Den%26client%3Dsafari".some
    )
  )
  val expected = Map(
    "event_vendor" -> "com.snowplowanalytics.snowplow",
    "event_name" -> "page_ping",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "page_ping",
    "refr_medium" -> "search",
    "refr_urlhost" -> "www.google.com",
    "refr_urlscheme" -> "http",
    "refr_urlquery" -> "q=%0Agateway%09oracle%09cards%09denise%09linn&hl=en&client=safari",
    "refr_dvce_tstamp" -> "",
    "refr_term" -> "gateway    oracle    cards    denise    linn",
    "refr_urlfragment" -> "",
    "refr_domain_userid" -> "",
    "refr_urlport" -> "80",
    "refr_urlpath" -> "/search",
    "refr_source" -> "Google"
  )
}

class RefererParserEnrichmentSpec extends PipelineSpec {
  import RefererParserEnrichmentSpec._
  "RefererParserEnrichment" should "enrich using the referer parser enrichment" in {
    val url =
      "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/referer-parser/referer-tests.json"
    val localFile = "./referer-parser.json"
    SpecHelpers.downloadLocalEnrichmentFile(url, localFile)

    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/referer_parser").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(DistCacheIO(url), List(Right(localFile)))
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()

    SpecHelpers.deleteLocalFile(localFile)
  }
}
