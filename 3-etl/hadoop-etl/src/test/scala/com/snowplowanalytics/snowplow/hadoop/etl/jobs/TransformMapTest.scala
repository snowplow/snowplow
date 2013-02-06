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
import scala.reflect.BeanProperty

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// This project
// TODO: remove this when Scalding 0.8.3 released
import utils.Json2Line
import TestHelpers._
import utils.DataTransform._
import enrichments.MiscEnrichments
import utils.ConversionUtils
import enrichments.EventEnrichments

// Test class
class Target {
  @BeanProperty var platform: String = _
  @BeanProperty var br_features_pdf: Byte = _
  @BeanProperty var visit_id: Int = _
  @BeanProperty var tracker_v: String = _
  @BeanProperty var dt: String = _
  @BeanProperty var tm: String = _
}

/**
 * Integration test for the EtlJob:
 *
 * Input data _is_ not in the
 * expected CloudFront format.
 */
class TransformMapTest extends Specification {

  "Executing a TransformMap against a SourceMap" should {
    "successfully set each of the target fields" in {

      val sourceMap = Map("p"      -> "web",
                          "f_pdf"  -> "1",
                          "vid"    -> "1",
                          "tv"     -> "no-js-0.1.0",
                          "tstamp" -> "2013-01-01 23-11-59")

      val transformMap: TransformMap = Map("p"      -> (!~(MiscEnrichments.extractPlatform), "platform"),
                                           "f_pdf"  -> (!~(ConversionUtils.stringToByte), "br_features_pdf"),
                                           "vid"    -> (!~(ConversionUtils.stringToInt), "visit_id"),
                                           "tv"     -> (!~(MiscEnrichments.identity), "tracker_v"),
                                           "tstamp" -> (!~(EventEnrichments.extractTimestamp), ("dt", "tm")))

      val expected = new Target().tap { t =>
        t.platform = "web"
        t.br_features_pdf = 1
        t.visit_id = 1
        t.tracker_v = "no-js-0.1.1"
        t.dt = "2013-01-01"
        t.tm = "23-11-59"
      }

      val target = new Target
      target.transform(sourceMap, transformMap)

      target.platform must_== expected.platform
      target.visit_id must_== expected.visit_id
      target.br_features_pdf must_== expected.br_features_pdf
      target.tracker_v = expected.tracker_v
      target.dt must_== expected.dt
      target.tm must_== expected.tm
    }
  }
}