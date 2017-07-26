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

// This project
import config.Step

/**
 * End-of-the-world result type.
 * Controls how RDB Loader exits
 */
sealed trait Log

object Log {

  /**
   * Loading succeeded. No messages, 0 exit code
   */
  case class LoadingSucceeded(steps: List[Step]) extends Log {
    override def toString: String = {
      s"RDB Loader successfully completed following steps: [${steps.mkString(", ")}]"
    }
  }

  /**
   * Loading failed. Write error message. 1 exit code.
   */
  case class LoadingFailed(error: String, steps: List[Step]) extends Log {
    override def toString: String = {
      s"ERROR: $error\n" + (if (steps.nonEmpty) {
        s"Following steps completed: [${steps.mkString(",")}]" }
      else {
        "No steps completed"
      })
    }
  }
}
