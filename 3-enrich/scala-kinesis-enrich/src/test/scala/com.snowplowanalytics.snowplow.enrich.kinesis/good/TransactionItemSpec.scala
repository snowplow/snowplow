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
import org.specs2.mutable.Specification
import org.specs2.execute.Result

// This project
import SpecHelpers._

object TransactionItemSpec {

  val raw = "CgABAAABQ/SiVfkLABQAAAAQc3NjLTAuMS4wLXN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAAByWU9dGkmdGlfaWQ9b3JkZXItMTIzJnRpX3NrPTEwMDImdGlfbmE9UmVkK3Nob2VzJnRpX3ByPTQwMDAmdGlfcXU9MSZ0aV9jdT1KUFkmZHRtPTEzOTEzNzg3MTYyNzUmdGlkPTQwMDAxNyZ2cD0xNjgweDQxNSZkcz0xNjgweDQxNSZ2aWQ9MjYmZHVpZD0zYzE3NTc1NDRlMzliY2E0JnA9d2ViJnR2PWpzLTAuMTMuMSZmcD0xODA0OTU0NzkwJmFpZD1DRmUyM2EmbGFuZz1lbi1VUyZjcz1VVEYtOCZ0ej1FdXJvcGUvTG9uZG9uJnVpZD1hbGV4KzEyMyZmX3BkZj0wJmZfcXQ9MSZmX3JlYWxwPTAmZl93bWE9MCZmX2Rpcj0wJmZfZmxhPTEmZl9qYXZhPTAmZl9nZWFycz0wJmZfYWc9MCZyZXM9MTkyMHgxMDgwJmNkPTI0JmNvb2tpZT0xJnVybD1maWxlOi8vZmlsZTovLy9Vc2Vycy9hbGV4L0RldmVsb3BtZW50L2Rldi1lbnZpcm9ubWVudC9kZW1vLzEtdHJhY2tlci9ldmVudHMuaHRtbC9vdmVycmlkZGVuLXVybC8ACwAtAAAACWxvY2FsaG9zdAsAMgAAAFFNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAPAEYLAAAABwAAABZDb25uZWN0aW9uOiBrZWVwLWFsaXZlAAACcENvb2tpZTogX191dG1hPTExMTg3MjI4MS44NzgwODQ0ODcuMTM5MDIzNzEwNy4xMzkwOTMxNTIxLjEzOTExMTA1ODIuNzsgX191dG16PTExMTg3MjI4MS4xMzkwMjM3MTA3LjEuMS51dG1jc3I9KGRpcmVjdCl8dXRtY2NuPShkaXJlY3QpfHV0bWNtZD0obm9uZSk7IF9zcF9pZC4xZmZmPWI4OWE2ZmE2MzFlZWZhYzIuMTM5MDIzNzEwNy43LjEzOTExMTE4MTkuMTM5MDkzMTU0NTsgaGJsaWQ9Q1BqanVodkYwNXprdFA3SjdNNVZvM05JR1BMSnkxU0Y7IG9sZnNrPW9sZnNrNTYyOTIzNjM1NjE3NTU0OyBzcD03NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQ7IHdjc2lkPUtSaGhrNEhFTHAyQWlwcUw3TTVWb25DUE9QeUFuRjFKOyBfb2tsdj0xMzkxMTExNzc5MzI4JTJDS1JoaGs0SEVMcDJBaXBxTDdNNVZvbkNQT1B5QW5GMUo7IF9fdXRtYz0xMTE4NzIyODE7IF9va2JrPWNkNCUzRHRydWUlMkN2aTUlM0QwJTJDdmk0JTNEMTM5MTExMDU4NTQ5MCUyQ3ZpMyUzRGFjdGl2ZSUyQ3ZpMiUzRGZhbHNlJTJDdmkxJTNEZmFsc2UlMkNjZDglM0RjaGF0JTJDY2Q2JTNEMCUyQ2NkNSUzRGF3YXklMkNjZDMlM0RmYWxzZSUyQ2NkMiUzRDAlMkNjZDElM0QwJTJDOyBfb2s9OTc1Mi01MDMtMTAtNTIyNwAAAB5BY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUAAAAaQWNjZXB0LUxhbmd1YWdlOiBlbi1VUywgZW4AAAArQWNjZXB0OiBpbWFnZS9wbmcsIGltYWdlLyo7cT0wLjgsICovKjtxPTAuNQAAAF1Vc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAAAAAUSG9zdDogbG9jYWxob3N0OjQwMDELAFAAAAAkNzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AA=="

  val expected = List(
    "CFe23a",
    "web",
    "2014-02-02 22:05:16.153",
    "2014-02-02 22:05:16.275",
    "transaction_item",
    "com.snowplowanalytics",
    Uuid4Regexp, // Regexp match
    "400017",
    "js-0.13.1",
    "ssc-0.1.0-stdout",
    EnrichVersion,
    "alex 123",
    "10.0.2.x",
    "1804954790",
    "3c1757544e39bca4",
    "26",
    "",
    "",
    "",
    "",
    "",
    "",
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
    "order-123",
    "1002",
    "Red shoes",
    "",
    "4000",
    "1",
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
    "415"
    )
}

class TransactionItemSpec extends Specification {

  "Scala Kinesis Enrich" should {

    "enrich a valid transaction item" in {

      val rawEvent = Base64.decodeBase64(TransactionItemSpec.raw)
      
      val enrichedEvent = TestSource.enrichEvent(rawEvent)
      enrichedEvent must beSome

      val fields = enrichedEvent.get.split("\t")
      fields.size must beEqualTo(TransactionItemSpec.expected.size)

      Result.unit(
        for (idx <- TransactionItemSpec.expected.indices) {
          fields(idx) must beFieldEqualTo(TransactionItemSpec.expected(idx), withIndex = idx)
        }
      )
    }
  }
}
