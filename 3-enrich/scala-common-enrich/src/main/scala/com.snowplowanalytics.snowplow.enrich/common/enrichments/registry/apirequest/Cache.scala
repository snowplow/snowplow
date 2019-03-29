/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import com.twitter.util.SynchronizedLruMap
import io.circe._
import org.joda.time.DateTime

/**
 * LRU cache
 * @param size amount of objects
 * @param ttl time in seconds to live
 */
final case class Cache(size: Int, ttl: Int) {

  // URI -> Validated[JSON]
  private val cache = new SynchronizedLruMap[String, (Either[Throwable, Json], Int)](size)

  /**
   * Get a value if it's not outdated
   * @param url HTTP URL
   * @return validated JSON as it was returned from API server
   */
  def get(url: String): Option[Either[Throwable, Json]] =
    cache.get(url) match {
      case Some((value, _)) if ttl == 0 => Some(value)
      case Some((value, created)) => {
        val now = (new DateTime().getMillis / 1000).toInt
        if (now - created < ttl) Some(value)
        else {
          cache.remove(url)
          None
        }
      }
      case _ => None
    }

  /**
   * Put a value into cache with current timestamp
   * @param key all inputs Map
   * @param value context object (with Iglu URI, not just plain JSON)
   */
  def put(key: String, value: Either[Throwable, Json]): Unit = {
    val now = (new DateTime().getMillis / 1000).toInt
    cache.put(key, (value, now))
    ()
  }

  /**
   * Get actual size of cache
   * @return number of elements in
   */
  private[apirequest] def actualLoad: Int = cache.size
}
