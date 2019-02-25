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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry
package sqlquery

// Scala
import scala.collection.immutable.IntMap

// json4s
import org.json4s.JObject

// JodaTime
import org.joda.time.DateTime

// Twitter utils
import com.twitter.util.SynchronizedLruMap

// This library
import Input.ExtractedValue

/**
 * Just LRU cache
 * Stores full IntMap with extracted values as keys and
 * full list Self-describing contexts as values
 *
 * @param size amount of objects
 * @param ttl time in seconds to live
 */
case class Cache(size: Int, ttl: Int) {

  private val cache = new SynchronizedLruMap[IntMap[ExtractedValue], (ThrowableXor[List[JObject]], Int)](size)

  /**
   * Get a value if it's not outdated
   *
   * @param key HTTP URL
   * @return validated JSON as it was fetched from DB if found
   */
  def get(key: IntMap[ExtractedValue]): Option[ThrowableXor[List[JObject]]] =
    cache.get(key) match {
      case Some((value, _)) if ttl == 0 => Some(value)
      case Some((value, created)) =>
        val now = (new DateTime().getMillis / 1000).toInt
        if (now - created < ttl) Some(value)
        else {
          cache.remove(key)
          None
        }
      case _ => None
    }

  /**
   * Put a value into cache with current timestamp
   *
   * @param key all inputs Map
   * @param value context object (with Iglu URI, not just plain JSON)
   */
  def put(key: IntMap[ExtractedValue], value: ThrowableXor[List[JObject]]): Unit = {
    val now = (new DateTime().getMillis / 1000).toInt
    cache.put(key, (value, now))
  }

  /**
   * Get actual size of cache
   *
   * @return number of elements in
   */
  private[sqlquery] def actualLoad: Int = cache.size
}
