/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package bad

import org.specs2.mutable.Specification

object CorruptedCfLinesSpec {
  val lines = EnrichJobSpec.Lines(
    "20yy-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  -"
  )
  val expected = """{"line":"20yy-05-24  00:08:40  LHR5  3397  74.125.17.210 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html Mozilla/5.0%20(Linux;%20U;%20Android%202.3.4;%20generic)%20AppleWebKit/535.1%20(KHTML,%20like%20Gecko;%20Google%20Web%20Preview)%20Version/4.0%20Mobile%20Safari/535.1  -","errors":[{"level":"error","message":"Unexpected exception converting date [20yy-05-24] and time [00:08:40] to timestamp: [Invalid format: \"20yy-05-24T00:08:40+00:00\" is malformed at \"yy-05-24T00:08:40+00:00\"]"}]}"""
}

/** Input data _is_ in the CloudFront access log format, but the fields are somehow corrupted. */
class CorruptedCfLinesSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "corrupted-cf-lines"
  sequential
  "A job which processes a corrupted input line" should {
    runEnrichJob(CorruptedCfLinesSpec.lines, "cloudfront", "1", false, List("geo"))

    "write a bad row JSON containing the input line and all errors" in {
      val Some(bads) = readPartFile(dirs.badRows)
      removeTstamp(bads.head) must_== CorruptedCfLinesSpec.expected
    }

    "not write any events" in {
      dirs.output must beEmptyDir
    }
  }
}
