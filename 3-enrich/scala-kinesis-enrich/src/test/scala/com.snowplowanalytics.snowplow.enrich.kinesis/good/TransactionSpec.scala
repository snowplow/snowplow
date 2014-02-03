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

object TransactionSpec {

  val raw = "CgABAAABQ/SiVe8LABQAAAAQc3NjLTAuMS4wLXN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAABpmU9dHImdHJfaWQ9b3JkZXItMTIzJnRyX3R0PTgwMDAmdHJfY3U9SlBZJmR0bT0xMzkxMzc4NzE2MjcxJnRpZD02MzYyMzkmdnA9MTY4MHg0MTUmZHM9MTY4MHg0MTUmdmlkPTI2JmR1aWQ9M2MxNzU3NTQ0ZTM5YmNhNCZwPXdlYiZ0dj1qcy0wLjEzLjEmZnA9MTgwNDk1NDc5MCZhaWQ9Q0ZlMjNhJmxhbmc9ZW4tVVMmY3M9VVRGLTgmdHo9RXVyb3BlL0xvbmRvbiZ1aWQ9YWxleCsxMjMmZl9wZGY9MCZmX3F0PTEmZl9yZWFscD0wJmZfd21hPTAmZl9kaXI9MCZmX2ZsYT0xJmZfamF2YT0wJmZfZ2VhcnM9MCZmX2FnPTAmcmVzPTE5MjB4MTA4MCZjZD0yNCZjb29raWU9MSZ1cmw9ZmlsZTovL2ZpbGU6Ly8vVXNlcnMvYWxleC9EZXZlbG9wbWVudC9kZXYtZW52aXJvbm1lbnQvZGVtby8xLXRyYWNrZXIvZXZlbnRzLmh0bWwvb3ZlcnJpZGRlbi11cmwvAAsALQAAAAlsb2NhbGhvc3QLADIAAABRTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTAuOTsgcnY6MjYuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8yNi4wDwBGCwAAAAcAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAAnBDb29raWU6IF9fdXRtYT0xMTE4NzIyODEuODc4MDg0NDg3LjEzOTAyMzcxMDcuMTM5MDkzMTUyMS4xMzkxMTEwNTgyLjc7IF9fdXRtej0xMTE4NzIyODEuMTM5MDIzNzEwNy4xLjEudXRtY3NyPShkaXJlY3QpfHV0bWNjbj0oZGlyZWN0KXx1dG1jbWQ9KG5vbmUpOyBfc3BfaWQuMWZmZj1iODlhNmZhNjMxZWVmYWMyLjEzOTAyMzcxMDcuNy4xMzkxMTExODE5LjEzOTA5MzE1NDU7IGhibGlkPUNQamp1aHZGMDV6a3RQN0o3TTVWbzNOSUdQTEp5MVNGOyBvbGZzaz1vbGZzazU2MjkyMzYzNTYxNzU1NDsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0OyB3Y3NpZD1LUmhoazRIRUxwMkFpcHFMN001Vm9uQ1BPUHlBbkYxSjsgX29rbHY9MTM5MTExMTc3OTMyOCUyQ0tSaGhrNEhFTHAyQWlwcUw3TTVWb25DUE9QeUFuRjFKOyBfX3V0bWM9MTExODcyMjgxOyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTExMTA1ODU0OTAlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgX29rPTk3NTItNTAzLTEwLTUyMjcAAAAeQWNjZXB0LUVuY29kaW5nOiBnemlwLCBkZWZsYXRlAAAAGkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuAAAAK0FjY2VwdDogaW1hZ2UvcG5nLCBpbWFnZS8qO3E9MC44LCAqLyo7cT0wLjUAAABdVXNlci1BZ2VudDogTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTAuOTsgcnY6MjYuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8yNi4wAAAAFEhvc3Q6IGxvY2FsaG9zdDo0MDAxCwBQAAAAJDc1YTEzNTgzLTVjOTktNDBlMy04MWZjLTU0MTA4NGRmYzc4NAA="

  val expected = List(
    "CFe23a",
    "web",
    "2014-02-02 22:05:16.143",
    "2014-02-02 22:05:16.271",
    "transaction",
    "com.snowplowanalytics",
    Uuid4Regexp, // Regexp match
    "636239",
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
    "order-123",
    "",
    "8000",
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
    "415"
    )
}

class TransactionSpec extends Specification {

  "Scala Kinesis Enrich" should {

    "enrich a valid transaction" in {

      val rawEvent = Base64.decodeBase64(TransactionSpec.raw)
      
      val enrichedEvent = TestSource.enrichEvent(rawEvent)
      enrichedEvent must beSome

      val fields = enrichedEvent.get.split("\t")
      fields.size must beEqualTo(TransactionSpec.expected.size)

      Result.unit(
        for (idx <- TransactionSpec.expected.indices) {
          fields(idx) must beFieldEqualTo(TransactionSpec.expected(idx), withIndex = idx)
        }
      )
    }
  }
}
