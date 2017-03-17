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
package misc

import org.specs2.mutable.Specification

object DiscardableCfLinesSpec {
  val lines = EnrichJobSpec.Lines(
    "#Version: 1.0",
    "#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query"
  )
}

/** CloudFront-format rows which should be discarded. */
class DiscardableCfLinesSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "discardable-cf-lines"
  sequential
  "A job which processes expected but discardable CloudFront input lines" should {
    runEnrichJob(DiscardableCfLinesSpec.lines, "cloudfront", "1", false, List("geo"))

    "not write any events" in {
      dirs.output must beEmptyDir
    }
    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
