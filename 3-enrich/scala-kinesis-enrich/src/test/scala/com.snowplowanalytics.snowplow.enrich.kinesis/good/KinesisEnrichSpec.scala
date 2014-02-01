/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich.kinesis
package good

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.execute.Result
import org.specs2.scalaz.ValidationMatchers

// This project
import SpecHelpers._

object KinesisEnrichSpec {

  val raw = "CgABAAABQ9qNGa4LABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAACeWU9dWUmdWVfbmE9Vmlld2VkK1Byb2R1Y3QmdWVfcHI9JTdCJTIycHJvZHVjdF9pZCUyMjolMjJBU08wMTA0MyUyMiwlMjJjYXRlZ29yeSUyMjolMjJEcmVzc2VzJTIyLCUyMmJyYW5kJTIyOiUyMkFDTUUlMjIsJTIycmV0dXJuaW5nJTIyOnRydWUsJTIycHJpY2UlMjI6NDkuOTUsJTIyc2l6ZXMlMjI6JTVCJTIyeHMlMjIsJTIycyUyMiwlMjJsJTIyLCUyMnhsJTIyLCUyMnh4bCUyMiU1RCwlMjJhdmFpbGFibGVfc2luY2UkZHQlMjI6MTU4MDElN0QmZHRtPTEzOTA5NDExMTUyNjMmdGlkPTY0NzYxNSZ2cD0yNTYweDk2MSZkcz0yNTYweDk2MSZ2aWQ9OCZkdWlkPTNjMTc1NzU0NGUzOWJjYTQmcD1tb2ImdHY9anMtMC4xMy4xJmZwPTI2OTU5MzA4MDMmYWlkPUNGZTIzYSZsYW5nPWVuLVVTJmNzPVVURi04JnR6PUV1cm9wZS9Mb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0yNTYweDE0NDAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGU6Ly9maWxlOi8vL1VzZXJzL2FsZXgvRGV2ZWxvcG1lbnQvZGV2LWVudmlyb25tZW50L2RlbW8vMS10cmFja2VyL2V2ZW50cy5odG1sL292ZXJyaWRkZW4tdXJsLwALAC0AAAAJbG9jYWxob3N0CwAyAAAAUU1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMA8ARgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsAUAAAACQ3NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQA"

  val expected = List(
    "",
    "",
    "2014-01-16 00:49:58.278",
    "",
    "",
    "com.snowplowanalytics",
    Uuid4Regexp,
    "",
    "",
    EnrichVersion,
    "kinesis-0.1.0-common-0.2.0-SNAPSHOT",
    "",
    "127.0.0.x",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36",
    "Chrome 31",
    "Chrome",
    "31.0.1650.63",
    "Browser",
    "WEBKIT",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "Linux",
    "Linux",
    "Other",
    "",
    "Computer",
    "0"
    )
}

class KinesisEnrichSpec extends Specification with AnyMatchers {

  "Scala Kinesis Enrich" should {

    "enrich a valid SnowplowRawEvent" in {

      val rawEvent = Base64.decodeBase64(KinesisEnrichSpec.raw)
      
      val enrichedEvent = TestSource.enrichEvent(rawEvent)
      enrichedEvent must beSome

      val fields = enrichedEvent.get.split("\t")
      fields.size must beEqualTo(KinesisEnrichSpec.expected.size)

      Result.unit(
        for (idx <- KinesisEnrichSpec.expected.indices) {
          fields(idx) must beFieldEqualTo(KinesisEnrichSpec.expected(idx), withIndex = idx)
        }
      )
    }
  }
}
