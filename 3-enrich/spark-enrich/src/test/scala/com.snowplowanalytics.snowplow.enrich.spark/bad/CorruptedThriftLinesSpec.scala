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

object CorruptedThriftLinesSpec {
  val expected = """{"line":"bac=","errors":[{"level":"error","message":"Error deserializing raw event: Cannot read. Remote side has closed. Tried to read 2 bytes, but only got 1 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"}]}"""
}

/** Input Thrift data cannot be decoded so should be base 64 encoded in the resulting bad row. */
class CorruptedThriftLinesSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "corrupted-thrift-lines"
  sequential
  "A job which processes a corrupted input line" should {
    runEnrichJob(getClass().getResource("CorruptedThriftLinesSpec.line.lzo").toString(), "thrift",
      "1", false, List("geo"), false, false, false, false)

    "write a bad row JSON containing the input line and all errors" in {
      val Some(bads) = readPartFile(dirs.badRows)
      removeTstamp(bads.head) must_== CorruptedThriftLinesSpec.expected
    }

    "not write any events" in {
      dirs.output must beEmptyDir
    }
  }
}
