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
package enrich.stream
package good

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.mutable.Specification
import org.specs2.execute.Result

// This project
import SpecHelpers._

object StructEventWithContextSpec {

  val raw = "CgABAAABQ/SgCPELABQAAAAQc3NjLTAuMS4wLXN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAAC4GU9c2Umc2VfY2E9Q2hlY2tvdXQmc2VfYWM9QWRkJnNlX2xhPUFTTzAxMDQzJnNlX3ByPWJsdWU6eHhsJnNlX3ZhPTIuMCZjeD1leUp6WTJobGJXRWlPaUpwWjJ4MU9tTnZiUzV6Ym05M2NHeHZkMkZ1WVd4NWRHbGpjeTV6Ym05M2NHeHZkeTlqYjI1MFpYaDBjeTlxYzI5dWMyTm9aVzFoTHpFdE1DMHdJaXdpWkdGMFlTSTZXM3NpYzJOb1pXMWhJam9pYVdkc2RUcGpiMjB1YzI1dmQzQnNiM2RoYm1Gc2VYUnBZM011YzI1dmQzQnNiM2N2ZDJWaVgzQmhaMlV2YW5OdmJuTmphR1Z0WVM4eExUQXRNQ0lzSW1SaGRHRWlPbnNpYVdRaU9pSmlNRFZpTXpGak15MDRNV0ZqTFRSaFpqVXRPVEprTVMweE1UTXhNek01TmpnMk5UVWlmWDFkZlEmZHRtPTEzOTEzNzg1NjU0OTMmdGlkPTc4MjQ0OSZ2cD0xNjgweDQxNSZkcz0xNjgweDQxNSZ2aWQ9MjYmZHVpZD0zYzE3NTc1NDRlMzliY2E0JnA9d2ViJnR2PWpzLTAuMTMuMSZmcD0xODA0OTU0NzkwJmFpZD1DRmUyM2EmbGFuZz1lbi1VUyZjcz1VVEYtOCZ0ej1FdXJvcGUvTG9uZG9uJnVpZD1hbGV4KzEyMyZmX3BkZj0wJmZfcXQ9MSZmX3JlYWxwPTAmZl93bWE9MCZmX2Rpcj0wJmZfZmxhPTEmZl9qYXZhPTAmZl9nZWFycz0wJmZfYWc9MCZyZXM9MTkyMHgxMDgwJmNkPTI0JmNvb2tpZT0xJnVybD1maWxlOi8vZmlsZTovLy9Vc2Vycy9hbGV4L0RldmVsb3BtZW50L2Rldi1lbnZpcm9ubWVudC9kZW1vLzEtdHJhY2tlci9ldmVudHMuaHRtbC9vdmVycmlkZGVuLXVybC8ACwAtAAAACWxvY2FsaG9zdAsAMgAAAFFNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAPAEYLAAAABwAAABZDb25uZWN0aW9uOiBrZWVwLWFsaXZlAAACcENvb2tpZTogX191dG1hPTExMTg3MjI4MS44NzgwODQ0ODcuMTM5MDIzNzEwNy4xMzkwOTMxNTIxLjEzOTExMTA1ODIuNzsgX191dG16PTExMTg3MjI4MS4xMzkwMjM3MTA3LjEuMS51dG1jc3I9KGRpcmVjdCl8dXRtY2NuPShkaXJlY3QpfHV0bWNtZD0obm9uZSk7IF9zcF9pZC4xZmZmPWI4OWE2ZmE2MzFlZWZhYzIuMTM5MDIzNzEwNy43LjEzOTExMTE4MTkuMTM5MDkzMTU0NTsgaGJsaWQ9Q1BqanVodkYwNXprdFA3SjdNNVZvM05JR1BMSnkxU0Y7IG9sZnNrPW9sZnNrNTYyOTIzNjM1NjE3NTU0OyBzcD03NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQ7IHdjc2lkPUtSaGhrNEhFTHAyQWlwcUw3TTVWb25DUE9QeUFuRjFKOyBfb2tsdj0xMzkxMTExNzc5MzI4JTJDS1JoaGs0SEVMcDJBaXBxTDdNNVZvbkNQT1B5QW5GMUo7IF9fdXRtYz0xMTE4NzIyODE7IF9va2JrPWNkNCUzRHRydWUlMkN2aTUlM0QwJTJDdmk0JTNEMTM5MTExMDU4NTQ5MCUyQ3ZpMyUzRGFjdGl2ZSUyQ3ZpMiUzRGZhbHNlJTJDdmkxJTNEZmFsc2UlMkNjZDglM0RjaGF0JTJDY2Q2JTNEMCUyQ2NkNSUzRGF3YXklMkNjZDMlM0RmYWxzZSUyQ2NkMiUzRDAlMkNjZDElM0QwJTJDOyBfb2s9OTc1Mi01MDMtMTAtNTIyNwAAAB5BY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUAAAAaQWNjZXB0LUxhbmd1YWdlOiBlbi1VUywgZW4AAAArQWNjZXB0OiBpbWFnZS9wbmcsIGltYWdlLyo7cT0wLjgsICovKjtxPTAuNQAAAF1Vc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAAAAAUSG9zdDogbG9jYWxob3N0OjQwMDELAFAAAAAkNzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AA=="

  val expected = List(
    "CFe23a",
    "web",
    TimestampRegex,
    "2014-02-02 22:02:45.361",
    "2014-02-02 22:02:45.493",
    "struct",
    Uuid4Regexp, // Regexp match
    "782449",
    "",
    "js-0.13.1",
    "ssc-0.1.0-stdout",
    EnrichVersion,
    "alex 123",
    "10.0.2.x",
    "1804954790",
    "3c1757544e39bca4",
    "26",
    "75a13583-5c99-40e3-81fc-541084dfc784",
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
    "file://file:///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/",
    "",
    "",
    "file",
    "file",
    "80",
    "///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"b05b31c3-81ac-4af5-92d1-113133968655"}}]}""",
    "Checkout",
    "Add",
    "ASO01043",
    "blue:xxl",
    "2.0",
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
    "Firefox 26",
    "Firefox",
    "26.0",
    "Browser",
    "GECKO",
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
    "Apple Inc.",
    "Europe/London",
    "Computer",
    "0",
    "1920",
    "1080",
    "UTF-8",
    "1680",
    "415",
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
    "2014-02-02 22:02:45.361",
    "com.google.analytics",
    "event",
    "jsonschema",
    "1-0-0",
    "",
    ""
    )
}

class StructEventWithContextSpec extends Specification {

  "Stream Enrich" should {

    "enrich a valid structured event with context" in {

      val rawEvent = Base64.decodeBase64(StructEventWithContextSpec.raw)

      val enrichedEvent = TestSource.enrichEvents(rawEvent)(0)
      enrichedEvent.isSuccess must beTrue

      // "-1" prevents empty strings from being discarded from the end of the array
      val fields = enrichedEvent.toOption.get._1.split("\t", -1)
      fields.size must beEqualTo(StructEventWithContextSpec.expected.size)

      Result.unit(
        for (idx <- StructEventWithContextSpec.expected.indices) {
          fields(idx) must beFieldEqualTo(StructEventWithContextSpec.expected(idx), withIndex = idx)
        }
      )
    }
  }
}
