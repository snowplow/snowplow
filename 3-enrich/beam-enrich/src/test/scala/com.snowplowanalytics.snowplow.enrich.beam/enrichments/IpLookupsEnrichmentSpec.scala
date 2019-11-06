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

import com.spotify.scio.ScioMetrics
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._
import org.apache.commons.codec.binary.Base64

object IpLookupsEnrichmentSpec {
  val raw = Seq(
    "CwBkAAAADTM3LjIyOC4yMjUuMzIKAMgAAAFjiJGp1QsA0gAAAAVVVEYtOAsA3AAAABJzc2MtMC4xMy4wLXN0ZG91dCQLASwAAAALY3VybC83LjUwLjMLAUAAAAAjL2NvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy90cDILAVQAAAFpeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9wYXlsb2FkX2RhdGEvanNvbnNjaGVtYS8xLTAtNCIsImRhdGEiOlt7InR2IjoidHJhY2tlcl92ZXJzaW9uIiwiZSI6InVlIiwicCI6IndlYiIsInVlX3ByIjoie1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9zY3JlZW5fdmlldy9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wibmFtZVwiOlwiaGVsbG8gZnJvbSBTbm93cGxvd1wifX19In1dfQ8BXgsAAAAFAAAAO0hvc3Q6IGVjMi0zNC0yNDUtMzItNDcuZXUtd2VzdC0xLmNvbXB1dGUuYW1hem9uYXdzLmNvbToxMjM0AAAAF1VzZXItQWdlbnQ6IGN1cmwvNy41MC4zAAAAC0FjY2VwdDogKi8qAAAAG1RpbWVvdXQtQWNjZXNzOiA8ZnVuY3Rpb24xPgAAABBhcHBsaWNhdGlvbi9qc29uCwFoAAAAEGFwcGxpY2F0aW9uL2pzb24LAZAAAAAwZWMyLTM0LTI0NS0zMi00Ny5ldS13ZXN0LTEuY29tcHV0ZS5hbWF6b25hd3MuY29tCwGaAAAAJDEwZDk2YmM3LWU0MDAtNGIyOS04YTQxLTY5MTFhZDAwZWU5OAt6aQAAAEFpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9Db2xsZWN0b3JQYXlsb2FkL3RocmlmdC8xLTAtMAA="
  )
  val expected = List(
    "web",
    "2018-05-22 15:57:17.653",
    "unstruct",
    "tracker_version",
    "ssc-0.13.0-stdout$",
    s"beam-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.sceVersion}",
    "37.228.225.32",
    "10d96bc7-e400-4b29-8a41-6911ad00ee98",
    "IE",
    "LH",
    "Dundalk",
    "A91",
    "53.999",
    "-6.4183",
    "Louth",
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/screen_view/jsonschema/1-0-0","data":{"name":"hello from Snowplow"}}}""",
    "curl/7.50.3",
    "com.snowplowanalytics.snowplow",
    "screen_view",
    "jsonschema",
    "1-0-0"
  )
}

class IpLookupsEnrichmentSpec extends PipelineSpec {
  import IpLookupsEnrichmentSpec._
  "IpLookupsEnrichment" should "enrich using the ip lookup enrichment (if failure, check coordinates)" in {
    val url =
      "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoLite2-City.mmdb"
    val localFile = "./ip_geo"
    SpecHelpers.downloadLocalEnrichmentFile(url, localFile)

    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()),
        "--enrichments=" + Paths.get(getClass.getResource("/ip_lookups").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw.map(Base64.decodeBase64))
      .distCache(DistCacheIO(url), List(Right(localFile)))
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          expected.forall(c.contains) // Add `println(c);` before `expected` to see the enrichment output
        }; ()
      }
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .distribution(Enrich.enrichedEventSizeDistribution) { d =>
        d.getCount() shouldBe 1
        d.getMin() shouldBe d.getMax()
        d.getMin() shouldBe d.getSum()
        d.getMin() shouldBe d.getMean()
        ()
      }
      .distribution(Enrich.timeToEnrichDistribution) { d =>
        d.getCount() shouldBe 1
        d.getMin() should be >= 100L
        d.getMin() shouldBe d.getMax()
        d.getMin() shouldBe d.getSum()
        d.getMin() shouldBe d.getMean()
        ()
      }
      .counter(ScioMetrics.counter("snowplow", "vendor_com_snowplowanalytics_snowplow")) { c =>
        c shouldBe 1
        ()
      }
      .counter(ScioMetrics.counter("snowplow", "tracker_tracker_version")) { c =>
        c shouldBe 1
        ()
      }
      .run()

    SpecHelpers.deleteLocalFile(localFile)
  }
}
