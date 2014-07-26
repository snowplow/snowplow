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
package com.snowplowanalytics.snowplow.enrich.common
package utils

// Scala
import scala.reflect.BeanProperty

// Scalaz
import scalaz._
import Scalaz._

// Specs2 & Scalaz-Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// Utils
import com.snowplowanalytics.util.Tap._
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder

// This project
import MapTransformer._
import enrichments.{MiscEnrichments, EventEnrichments, ClientEnrichments}

// Test Bean
final class TargetBean {
  @BeanProperty var platform: String = _
  @BeanProperty var br_features_pdf: Byte = _
  @BeanProperty var visit_id: Int = _
  @BeanProperty var tracker_v: String = _
  @BeanProperty var width: Int = _
  @BeanProperty var height: Int = _

  override def equals(other: Any): Boolean = other match {
    case that: TargetBean => {
      platform == that.platform &&
      br_features_pdf == that.br_features_pdf &&
      visit_id == that.visit_id &&
      tracker_v == that.tracker_v &&
      height == that.height &&
      width == that.width
    }
    case _ => false
  }
  // No canEqual needed as the class is final

  // Use Reflection - perf hit is okay as this is only in the test suite
  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this, false)
  override def toString: String = ToStringBuilder.reflectionToString(this)
}

/**
 * Tests the MapTransformer.
 */
class MapTransformerSpec extends Specification with ValidationMatchers {

  val sourceMap = Map("p"       -> "web",
                      "f_pdf"   -> "1",
                      "vid"     -> "1",
                      "tv"      -> "no-js-0.1.1",
                      "res"     -> "720x1080",
                      "missing" -> "Not in the transformation map")

  val transformMap: TransformMap = Map(("p"      , (MiscEnrichments.extractPlatform, "platform")),
                                       ("f_pdf"  , (ConversionUtils.stringToBooleanlikeJByte, "br_features_pdf")),
                                       ("vid"    , (ConversionUtils.stringToJInteger, "visit_id")),
                                       ("tv"     , (MiscEnrichments.identity, "tracker_v")),
                                       ("res"    , (ClientEnrichments.extractViewDimensions, ("width", "height"))))

  val expected = new TargetBean().tap { t =>
    t.platform = "web"
    t.br_features_pdf = 1
    t.visit_id = 1
    t.tracker_v = "no-js-0.1.1"
    t.width = 720
    t.height = 1080
  }

  "Applying a TransformMap to an existing POJO" should {
    "successfully set each of the target fields" in {

      val target = new TargetBean().tap { t =>
        t.platform = "old"
        t.tracker_v = "old"
      }
      val result = target.transform(sourceMap, transformMap)

      result must beSuccessful(6) // 6 fields updated
      target must_== expected
    }
  }

  "Executing TransformMap's generate() factory" should {
    "successfully instantiate a new POJO" in {

      val result = MapTransformer.generate[TargetBean](sourceMap, transformMap)
      result must beSuccessful(expected)
    }
  }
}