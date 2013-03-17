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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments

// Specs2
import org.specs2.mutable.Specification

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests the extractResolution function
 */
class ExtractResolutionTest extends Specification {

  val FieldName = "res"

  // TODO: switch to a data table for this test.
	"A resolution of 1200x800" should {
    "be successfully extracted" in {
      ClientEnrichments.extractResolution(FieldName, "1200x800") must_== (1200, 800).success
    }
  }
}