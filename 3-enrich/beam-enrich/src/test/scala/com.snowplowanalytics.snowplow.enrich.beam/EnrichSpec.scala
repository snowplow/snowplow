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

import java.nio.file.{Path, Paths}

import com.spotify.scio.ScioMetrics
import com.spotify.scio.testing._
import org.apache.commons.codec.binary.Base64

class EnrichSpec extends PipelineSpec {

  val raw = Seq("CgABAAABQ/Sevy0LABQAAAAQc3NjLTAuMS4wLXN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAABvGU9c2Umc2VfY2E9TWl4ZXMmc2VfYWM9UGxheSZzZV9sYT1NUkMvZmFicmljLTA1MDMtbWl4JnNlX3ZhPTAuMCZkdG09MTM5MTM3ODQ4MTA3MSZ0aWQ9MzQ0MjE0JnZwPTE2ODB4NDE1JmRzPTE2ODB4NDE1JnZpZD0yNiZkdWlkPTNjMTc1NzU0NGUzOWJjYTQmcD13ZWImdHY9anMtMC4xMy4xJmZwPTE4MDQ5NTQ3OTAmYWlkPUNGZTIzYSZsYW5nPWVuLVVTJmNzPVVURi04JnR6PUV1cm9wZS9Mb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0xOTIweDEwODAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGU6Ly9maWxlOi8vL1VzZXJzL2FsZXgvRGV2ZWxvcG1lbnQvZGV2LWVudmlyb25tZW50L2RlbW8vMS10cmFja2VyL2V2ZW50cy5odG1sL292ZXJyaWRkZW4tdXJsLwALAC0AAAAJbG9jYWxob3N0CwAyAAAAUU1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMA8ARgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA5MzE1MjEuMTM5MTExMDU4Mi43OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjcuMTM5MTExMTgxOS4xMzkwOTMxNTQ1OyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IHNwPTc1YTEzNTgzLTVjOTktNDBlMy04MWZjLTU0MTA4NGRmYzc4NDsgd2NzaWQ9S1JoaGs0SEVMcDJBaXBxTDdNNVZvbkNQT1B5QW5GMUo7IF9va2x2PTEzOTExMTE3NzkzMjglMkNLUmhoazRIRUxwMkFpcHFMN001Vm9uQ1BPUHlBbkYxSjsgX191dG1jPTExMTg3MjI4MTsgX29rYms9Y2Q0JTNEdHJ1ZSUyQ3ZpNSUzRDAlMkN2aTQlM0QxMzkxMTEwNTg1NDkwJTJDdmkzJTNEYWN0aXZlJTJDdmkyJTNEZmFsc2UlMkN2aTElM0RmYWxzZSUyQ2NkOCUzRGNoYXQlMkNjZDYlM0QwJTJDY2Q1JTNEYXdheSUyQ2NkMyUzRGZhbHNlJTJDY2QyJTNEMCUyQ2NkMSUzRDAlMkM7IF9vaz05NzUyLTUwMy0xMC01MjI3AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsAUAAAACQ3NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQA")
  val expected = List(
    "CFe23a",
    "web",
    "2014-02-02 22:01:20.941",
    "2014-02-02 22:01:21.071",
    "struct",
    "344214",
    "js-0.13.1",
    "ssc-0.1.0-stdout",
    s"beam-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.sceVersion}",
    "alex 123",
    "10.0.2.2",
    "1804954790",
    "3c1757544e39bca4",
    "26",
    "75a13583-5c99-40e3-81fc-541084dfc784",
    "file://file:///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/",
    "file",
    "file",
    "80",
    "///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/",
    "Mixes",
    "Play",
    "MRC/fabric-0503-mix",
    "0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
    "Firefox",
    "26.0",
    "en-US",
    "0",
    "1",
    "0",
    "0",
    "1",
    "0",
    "0",
    "0",
    "0",
    "1",
    "24",
    "1680",
    "415",
    "Mac OS X",
    "Mac OS X",
    "Europe/London",
    "0",
    "1920",
    "1080",
    "UTF-8",
    "1680",
    "415",
    "2014-02-02 22:01:20.941",
    "com.google.analytics",
    "event",
    "jsonschema",
    "1-0-0"
  )

  "Enrich" should "enrich a struct event" in {
    JobTest[Enrich.type]
      .args("--job-name=j", "--raw=in", "--enriched=out", "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()))
      .input(PubsubIO("in"), raw.map(Base64.decodeBase64))
      .distCache(DistCacheIO(""), List.empty[Either[String, Path]])
      .output(PubsubIO[String]("out"))(_ should satisfySingleValue { c: String =>
        expected.forall(c.contains)
      })
      .output(PubsubIO[String]("bad"))(_ should beEmpty)
      .distribution(Enrich.enrichedEventSizeDistribution) { d =>
        d.getCount() shouldBe 1
        d.getMin() shouldBe 814
        d.getMin() shouldBe d.getMax()
        d.getMin() shouldBe d.getSum()
        d.getMin() shouldBe d.getMean()
      }
      .distribution(Enrich.timeToEnrichDistribution) { d =>
        d.getCount() shouldBe 1
        d.getMin() should be >= 100L
        d.getMin() shouldBe d.getMax()
        d.getMin() shouldBe d.getSum()
        d.getMin() shouldBe d.getMean()
      }
      .counter(ScioMetrics.counter("snowplow", "vendor_com_google_analytics"))(_ shouldBe 1)
      .counter(ScioMetrics.counter("snowplow", "tracker_js_0_13_1"))(_ shouldBe 1)
      .run()
  }

}
