/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package utils

// Scalding
import com.twitter.scalding.Args

// Scalaz
import scalaz._
import Scalaz._

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

/**
 * Specs the explodeUri function
 */
class ScalazArgsSpec extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the ScalazArgs functionality"  ^
                                                                 p^
  "a Scalding Args should be pimped to a ScalazArgs as needed"    ! e1^
  "required keys should be successfully validated by ScalazArgs"  ! e2^
  "optional keys should be successfully validated by ScalazArgs"  ! e3^
  "for a required key, %3D should be decoded to ="                ! e4^
  "for an optional key, %3D should be decoded to ="               ! e5^
                                                                  end

  val scaldingArgs = Args(Array("--alpha", "123", "--beta", "456", "--delta", "789", "abc", "--hive", "run%3D2013-07-07"))

  import ScalazArgs._
  def e1 = scaldingArgs.requiredz("alpha") must beSuccessful("123")

  val scalazArgs = new ScalazArgs(scaldingArgs)
  def err: (String) => String = key => "".format(key)

  def e2 =
    "SPEC NAME"                       || "KEY"       | "EXPECTED"                                                      |
    "Required and is present"         !! "alpha"     ! "123".success                                                   |
    "Error, required but not present" !! "epsilon"   ! "Required argument [epsilon] not found".fail                    |
    "Error, values are a list"        !! "delta"     ! "List of values found for argument [delta], should be one".fail |> {
      (_, key, expected) =>
        scalazArgs.requiredz(key) must_== expected
    }

  def e3 =
    "SPEC NAME"                       || "KEY"       | "EXPECTED"                                                              |
    "Optional and is present"         !! "beta"      ! Some("456").success                                                     |
    "Optional and is not present"     !! "gamma"     ! None.success                                                            |
    "Error, values are a list"        !! "delta"     ! "List of values found for argument [delta], should be at most one".fail |> {
      (_, key, expected) =>
        scalazArgs.optionalz(key) must_== expected
    }

  // TODO: can remove these when future Scalding Args drops support for name=value arguments
  def e4 = scalazArgs.requiredz("hive") must_== "run=2013-07-07".success
  def e5 = scalazArgs.optionalz("hive") must_== Some("run=2013-07-07").success
}