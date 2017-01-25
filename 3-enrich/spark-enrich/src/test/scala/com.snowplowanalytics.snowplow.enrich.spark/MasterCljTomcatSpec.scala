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

import org.specs2.mutable.Specification

object MasterCljTomcatSpec {
  // Concatenate ALL lines from ALL other jobs
  val lines = good.CljTomcatTp1SingleEventSpec.lines.lines ++  // 1 good
              good.CljTomcatCallrailEventSpec.lines.lines ++   // 1 good
              good.CljTomcatTp2MultiEventsSpec.lines.lines ++  // 3 good
              good.CljTomcatTp2MegaEventsSpec.lines.lines      // 7,500 good = 7,505 GOOD
  object expected {
    val goodCount = 7505
  }
}

/** Master test which runs using all of the individual good, bad and misc tests */
class MasterCljTomcatSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "master-clj-tomcat"
  sequential
  "A job which processes a Clojure-Tomcat file containing 7,505 valid events, 0 bad lines and " +
  "3 discardable lines" should {
    runEnrichJob(Lines(MasterCljTomcatSpec.lines: _*), "clj-tomcat", "1", false, List("geo"))

    "write 7,505 events" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== MasterCljTomcatSpec.expected.goodCount
    }

    "write 0 bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
