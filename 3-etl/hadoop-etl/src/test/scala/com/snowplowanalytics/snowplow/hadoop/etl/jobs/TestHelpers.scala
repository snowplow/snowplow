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
package jobs

// Scala
import scala.collection.mutable.ListBuffer

// Specs2
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._

// Scalding
import com.twitter.scalding._

/**
 * Holds helpers for running integration
 * tests on SnowPlow EtlJobs.
 */
object TestHelpers {

	val beEmpty: Matcher[ListBuffer[_]]  = ((_: ListBuffer[_]).isEmpty, "is not empty")

  // Standard JobTest definition used by all integration tests
  val EtlJobTest = 
    JobTest("com.snowplowanalytics.snowplow.hadoop.etl.EtlJob").
      arg("INPUT_FOLDER", "inputFolder").
      arg("INPUT_FORMAT", "cloudfront").
      arg("OUTPUT_FOLDER", "outputFolder").
      arg("ERRORS_FOLDER", "errorFolder").
      arg("CONTINUE_ON", "1")
}