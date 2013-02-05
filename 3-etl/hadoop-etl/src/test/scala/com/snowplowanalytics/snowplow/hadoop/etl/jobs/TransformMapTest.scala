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

// Test class
class Target {
  @BeanProperty var platform: String = _
  @BeanProperty var visit_id: String = _ // TODO: turn into Int = _
  @BeanProperty var br_features_pdf: String = _ // TODO: turn in Byte = _
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

      val sourceMap: SourceMap1 = Map("p"     -> "web",
                                      "f_pdf" -> "1",
                                      "vid"   -> "1")

      val transformMap: TransformMap1 = Map("p"     -> (identity, "platform"),
                                            "f_pdf" -> (identity, "supports_pdf"),
                                            "vid"   -> (identity, "visit_id"))

      val expected = new Target().tap { t =>
        t.platform = "web"
        t.visit_id = "1"
        t.br_features_pdf = "1"
      }

      val target = new Target

      target.platform must_== expected.platform
      target.visit_id must_== expected.visit_id
      target.br_features_pdf must_== expected.br_features_pdf
    }
  }
}