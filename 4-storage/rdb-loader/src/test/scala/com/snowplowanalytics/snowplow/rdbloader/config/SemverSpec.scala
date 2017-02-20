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
package config

import cats.implicits._

// specs2
import org.specs2.Specification

class SemverSpec extends Specification { def is = s2"""
  Decode valid semantic versions $e1
  Fail to decode invalid versions $e2
  Order valid semantic versions $e3
  """

  import Semver._

  def e1 = {
    val semverList = List("0.1.0-M1", "1.12.1-rc1", "0.0.1", "1.2.0", "10.10.10-rc8")
    val expected: List[Either[String, Semver]] = List(
      Semver(0,1,0, Some(Milestone(1))),
      Semver(1,12,1, Some(ReleaseCandidate(1))),
      Semver(0,0,1),
      Semver(1,2,0),
      Semver(10,10,10, Some(ReleaseCandidate(8)))
    ).map(Right.apply)

    val result = semverList.map(Semver.decodeSemver)
    result must beEqualTo(expected)
  }

  def e2 = {
    val invalidSemverList = List("1.0-M1", "-1.12.1-rc1", "0.2", "s.t.r", "-rc2")
    val result = invalidSemverList.map(Semver.decodeSemver)

    result must contain((semver: Either[String, Semver]) => semver.isLeft).forall
  }

  def e3 = {
    val orders = List(
      Semver(0,1,0) < Semver(0,2,0),
      Semver(0,2,1) < Semver(0,2,2),
      Semver(1,2,1) > Semver(1,2,0),
      Semver(1,2,1, Some(ReleaseCandidate(1))) < Semver(1,2,1),
      Semver(1,2,1, Some(Milestone(1))) < Semver(1,2,1),
      Semver(1,2,1, Some(Unknown(""))) < Semver(1,2,1),
      Semver(1,2,1, Some(Unknown("bar"))) != Semver(1,2,1, Some(Unknown("foo"))),
      Semver(1,2,1, Some(Unknown("bar"))) == Semver(1,2,1, Some(Unknown("bar")))
    )

    orders must contain((b: Boolean) => b == true).forall
  }
}
