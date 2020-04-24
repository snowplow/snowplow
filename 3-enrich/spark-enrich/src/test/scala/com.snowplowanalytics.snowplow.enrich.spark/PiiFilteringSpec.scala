/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import org.specs2.mutable.Specification

/** Pii filtering test which runs using all of the individual good, bad and misc tests */
class PiiFilteringSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "pii-filter-cf"
  sequential
  "A job which processes a CloudFront file" should {
    "filter out pii events" in {
      runEnrichJob(Lines(MasterCfSpec.lines: _*), "cloudfront", "1", false, List("geo"))
      val Some(goods) = readPartFile(dirs.output)
      val Some(bads)  = readPartFile(dirs.badRows)
      goods must not(contain(matching(".*pii.*")))
      bads must not(contain(matching(".*pii.*")))
    }
  }
}
