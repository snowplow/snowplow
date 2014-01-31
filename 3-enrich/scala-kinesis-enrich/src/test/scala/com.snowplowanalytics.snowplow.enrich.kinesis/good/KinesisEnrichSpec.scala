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

// specs2 testing libraries
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.execute.Result
import org.specs2.scalaz.ValidationMatchers

object KinesisEnrichSpec {

  val raw = "CgABAAABQ5iGqAYLABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAkxMjcuMC4wLjEMACkIAAEAAAABCAACAAAAAQsAAwAAABh0ZXN0UGFyYW09MyZ0ZXN0UGFyYW0yPTQACwAtAAAACTEyNy4wLjAuMQsAMgAAAGhNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNg8ARgsAAAAIAAAAL0Nvb2tpZTogc3A9YzVmM2EwOWYtNzVmOC00MzA5LWJlYzUtZmVhNTYwZjc4NDU1AAAAGkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuAAAAJEFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZSwgc2RjaAAAAHRVc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNgAAAFZBY2NlcHQ6IHRleHQvaHRtbCwgYXBwbGljYXRpb24veGh0bWwreG1sLCBhcHBsaWNhdGlvbi94bWw7cT0wLjksIGltYWdlL3dlYnAsICovKjtxPTAuOAAAABhDYWNoZS1Db250cm9sOiBtYXgtYWdlPTAAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAABRIb3N0OiAxMjcuMC4wLjE6ODA4MAsAUAAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUA"

  val expected = Array[String](
        "", "",
        "2014-01-16 00:49:58.278",
        "", "",
        "com.snowplowanalytics",
        SpecHelpers.uuid4Regexp,
        "", "",
        "ssc-0.0.1-Stdout",
        "kinesis-0.1.0-common-0.2.0-SNAPSHOT",
        "",
        "127.0.0.x",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36",
        "Chrome 31",
        "Chrome",
        "31.0.1650.63",
        "Browser",
        "WEBKIT",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "Linux",
        "Linux",
        "Other",
        "",
        "Computer",
        "0"
      )
}

class KinesisEnrichSpec extends Specification with AnyMatchers {

  "Snowplow's Kinesis enricher" should {

    "enrich a valid SnowplowRawEvent" in {

      val rawEvent = Base64.decodeBase64(KinesisEnrichSpec.raw)
      val enrichedEvent = SpecHelpers.testSource.enrichEvent(rawEvent)
      
      enrichedEvent must beLike { case Some(ev) =>
        
        val fields = ev.split("\t")
        fields.size must beEqualTo(KinesisEnrichSpec.expected.size)

        for (idx <- KinesisEnrichSpec.expected.indices) {
          if (SpecHelpers.uuid4Fields.contains(idx)) {
            fields(idx) must beMatching(KinesisEnrichSpec.expected(idx).r)
          } else {
            fields(idx) must beEqualTo(KinesisEnrichSpec.expected(idx))
          }
        }
      }
    }
  }
}
