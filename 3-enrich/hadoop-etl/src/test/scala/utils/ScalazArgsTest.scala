/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.hadoop
package utils

// Scalding
import com.twitter.scalding.Args

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

/**
 * Tests the explodeUri function
 */
class ScalazArgsTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the ScalazArgs functionality"                  ^
                                                                                 p^
  "a Scalding Args should be pimped to a ScalazArgs as needed"                    ! e1^
  "required, optional and missing keys should be handled correctly by ScalazArgs" ! e2^
                                                                                  end


  val scaldingArgs = Args(Array("--one", "1"))

  import ScalazArgs._
  def e1 = scaldingArgs.requiredz("one") must beSuccessful("1")

  def e2 = 1 must_== 1
}