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
package utils

// Scala
import scala.reflect.BeanProperty

// Scalaz
import scalaz._
import Scalaz._

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// This project
import MapTransformer._
import enrichments.{MiscEnrichments, EventEnrichments}

// Test Bean
class TargetBean {
  @BeanProperty var platform: String = _
  @BeanProperty var br_features_pdf: Byte = _
  @BeanProperty var visit_id: Int = _
  @BeanProperty var tracker_v: String = _
  @BeanProperty var dt: String = _
  @BeanProperty var tm: String = _
}

// Test Case Class
class TargetCC(
  platform: String,
  br_features_pdf: Byte,
  visit_id: Int,
  tracker_v: String,
  dt: String,
  tm: String
  )

// TODO: rename file

/**
 * Tests the MapTransformer.
 */
class MapTransformerTest extends Specification {

  val sourceMap = Map("p"       -> "web",
                      "f_pdf"   -> "1",
                      "vid"     -> "1",
                      "tv"      -> "no-js-0.1.0",
                      "tstamp"  -> "2013-01-01 23-11-59",
                      "missing" -> "Not in the transformation map")

  val transformMap: TransformMap = Map(("p"      , (MiscEnrichments.extractPlatform, "platform")),
                                       ("f_pdf"  , (ConversionUtils.stringToByte, "br_features_pdf")),
                                       ("vid"    , (ConversionUtils.stringToInt, "visit_id")),
                                       ("tv"     , (MiscEnrichments.identity, "tracker_v")),
                                       ("tstamp" , (EventEnrichments.extractTimestamp, ("dt", "tm"))))

  val expected = new TargetBean().tap { t =>
    t.platform = "web"
    t.br_features_pdf = 1
    t.visit_id = 1
    t.tracker_v = "no-js-0.1.1"
    t.dt = "2013-01-01"
    t.tm = "23-11-59"
  }

  "Applying a TransformMap to an existing POJO" should {
    "successfully set each of the target fields" in {

      val target = new TargetBean().tap { t =>
        t.platform = "deleteme"
        t.tracker_v = "deleteme"
      }
      val result = target.transform(sourceMap, transformMap)

      result must_== 6.successNel[String] // 6 fields updated

      target.platform must_== expected.platform
      target.visit_id must_== expected.visit_id
      target.br_features_pdf must_== expected.br_features_pdf
      target.tracker_v = expected.tracker_v
      target.dt must_== expected.dt
      target.tm must_== expected.tm
    }
  }

  /*
  "Executing TransformMap's generate() factory" should {
    "successfully instantiate a new POJO" in {

      val result = MapTransformer.generate[TargetBean](sourceMap, transformMap)
      result must_== expected.successNel[String]
    }
  } */

  // TODO: search for newInstance(args:_*

  "Executing TransformMap's generate() factory" should {
    "successfully instantiate a new case class" in {

      val result = MapTransformer.generate[TargetCC](sourceMap, transformMap)
      result must_== expected.successNel[String]
    }
  }
}