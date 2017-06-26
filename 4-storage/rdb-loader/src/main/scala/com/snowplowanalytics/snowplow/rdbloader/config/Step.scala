/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

// This project
import utils.Common._

/**
 * Step is part of loading process or result SQL-statement
 */
sealed trait Step extends StringEnum with Product with Serializable

object Step {

  /**
   * Step that will be skipped if not include it explicitly
   */
  sealed trait IncludeStep extends Step
  case object Vacuum extends IncludeStep { def asString = "vacuum" }

  /**
   * Step that will be included if not skip it explicitly
   */
  sealed trait SkipStep extends Step with StringEnum
  case object Download extends SkipStep { def asString = "download" }
  case object Analyze extends SkipStep { def asString = "analyze" }
  case object Delete extends SkipStep { def asString = "delete" }
  case object Shred extends SkipStep { def asString = "shred" }
  case object Load extends SkipStep { def asString = "load" }
  case object Discover extends SkipStep { def asString = "discover" }


  implicit val optionalStepRead =
    scopt.Read.reads { (fromString[IncludeStep](_)).andThen { s => s match {
      case Right(x) => x
      case Left(e) => throw new RuntimeException(e)
    } } }

  implicit val skippableStepRead =
    scopt.Read.reads { (fromString[SkipStep](_)).andThen { s => s match {
      case Right(x) => x
      case Left(e) => throw new RuntimeException(e)
    } } }

  /**
   * Steps included into app by default
   */
  val defaultSteps = sealedDescendants[SkipStep]

  /**
   * Remove explicitly disabled steps and add optional steps
   * to default set of steps
   *
   * @param toSkip enabled by-default steps
   * @param toInclude disabled by-default steps
   * @return end set of steps
   */
  def constructSteps(toSkip: Set[SkipStep], toInclude: Set[IncludeStep]): Set[Step] = {
    defaultSteps -- toSkip ++ toInclude
  }
}
