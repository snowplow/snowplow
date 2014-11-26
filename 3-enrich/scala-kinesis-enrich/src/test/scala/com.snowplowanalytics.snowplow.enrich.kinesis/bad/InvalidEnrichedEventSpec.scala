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
package bad

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.mutable.Specification

// This project
import SpecHelpers._

object InvalidEnrichedEventSpec {

  val raw = "CgABAAABSVEMALYLABQAAAAQc3NjLTAuMi4wLXN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAkxMjcuMC4wLjEMACkIAAEAAAABCAACAAAAAQsAAwAAAA1lPW5vbmV4aXN0ZW50AAsALQAAAAlsb2NhbGhvc3QLADIAAABjY3VybC83LjIyLjAgKHg4Nl82NC1wYy1saW51eC1nbnUpIGxpYmN1cmwvNy4yMi4wIE9wZW5TU0wvMS4wLjEgemxpYi8xLjIuMy40IGxpYmlkbi8xLjIzIGxpYnJ0bXAvMi4zDwBGCwAAAAMAAAALQWNjZXB0OiAqLyoAAAAUSG9zdDogbG9jYWxob3N0OjgwODAAAABvVXNlci1BZ2VudDogY3VybC83LjIyLjAgKHg4Nl82NC1wYy1saW51eC1nbnUpIGxpYmN1cmwvNy4yMi4wIE9wZW5TU0wvMS4wLjEgemxpYi8xLjIuMy40IGxpYmlkbi8xLjIzIGxpYnJ0bXAvMi4zCwBQAAAAJGE1YWUwMWQ1LTQ5NTctNGQyZS1iMzRjLTM4YTU1ZDExNGZlMQA="
}

class InvalidEnrichedEventSpec extends Specification {

  // TODO: update this after https://github.com/snowplow/snowplow/issues/463
  "Scala Kinesis Enrich" should {

    "return None for a valid SnowplowRawEvent which fails enrichment" in {

      val rawEvent = Base64.decodeBase64(InvalidEnrichedEventSpec.raw)
      
      val enrichedEvent = TestSource.enrichEvents(rawEvent)(0)
      enrichedEvent must beNone
    }
  }
}
