/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import org.scalacheck.Gen

import org.specs2.{ScalaCheck, Specification}


class S3Spec extends Specification with ScalaCheck { def is = s2"""
  Always extract atomic events path from valid S3 key $e1
  """

  def e1 = {

    val twoThousandTens =
      Gen.chooseNum(1348368300000L, 1569206700000L)

    prop { (timestamp: Long, file: String) =>
      val dateTime = new DateTime(timestamp)
      val format = DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss")
      val id = format.print(dateTime)
      val runId = s"s3://bucket/run=$id/atomic-events/$file"
      val s3Key = S3.Key.coerce(runId)
      S3.getAtomicPath(s3Key) must beSome.like {
        case folder => folder must endWith("atomic-events/")
      }
    }.setGen1(twoThousandTens).setGen2(Gen.alphaStr)
  }
}

