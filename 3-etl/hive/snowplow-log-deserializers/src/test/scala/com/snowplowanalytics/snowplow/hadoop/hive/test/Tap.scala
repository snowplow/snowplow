/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive
package test

// TODO: move this into SnowPlow Utils (new repo)

/**
 * Contains an implicit conversion to make any object tappable.
 */
object Tap {
  implicit def anyToTap[A](underlying: A) = new Tap(underlying)
}

/**
 * Adds Ruby-style tap method to Scala.
 *
 * Example usage:
 *
 * import Tap._
 * val order = new Order().tap { o =>
 *   o.setStatus('UNPAID')
 * }
 *
 * See also:
 * - http://www.naildrivin5.com/blog/2012/06/22/tap-versus-intermediate-variables.html
 * - http://stackoverflow.com/questions/3241101/with-statement-equivalent-for-scala
 */
class Tap[A](underlying: A) {

  /**
   * Ruby-style tap method
   */
  def tap(func: A => Unit): A = {
    func(underlying)
    underlying
  }
}