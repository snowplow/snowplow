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

object InvalidCfLinesSpec {
  val lines = EnrichJobSpec.Lines(
    "",
    "NOT VALID",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
  )
  val expected = (line: String) =>
    """{"line":"%s","errors":[{"level":"error","message":"Line does not match CloudFront header or data row formats"}]}"""
      .format(line)
}

/** Input data _is_ not in the expected CloudFront format. */
class InvalidCfLinesSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "invalid-cf-lines"
  sequential
  "A job which processes input lines not in CloudFront format" should {
    runEnrichJob(InvalidCfLinesSpec.lines, "cloudfront", "1", false, List("geo"))

    "write a bad row JSON with input line and error message for each input line" in {
      val Some(bads) = readPartFile(dirs.badRows)
      bads.map(removeTstamp) must_== InvalidCfLinesSpec.lines.lines.map(InvalidCfLinesSpec.expected)
    }

    "not write any events" in {
      dirs.output must beEmptyDir
    }
  }
}
