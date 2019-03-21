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

import cats.syntax.either._
import io.circe._
import org.specs2.Specification

class CacheSpec extends Specification {
  def is = s2"""
  This is a specification to test the API Request enrichment cache
  Update on identical URLs           $e1
  Preserve ttl of cache              $e2
  Remove unused value                $e3
  Update on identical URLs with body $e4
  """

  def e1 = {
    val cache = Cache(3, 2)
    val key = ApiRequestEnrichment.cacheKey(url = "http://api.acme.com/url", body = None)

    cache.put(key, Json.fromInt(42).asRight)
    cache.put(key, Json.fromInt(52).asRight)
    cache.get(key) must beSome.like {
      case v => v must beRight(Json.fromInt(52))
    } and (cache.actualLoad must beEqualTo(1))
  }

  def e2 = {
    val cache = Cache(3, 2)
    val key = ApiRequestEnrichment.cacheKey(url = "http://api.acme.com/url", body = None)
    cache.put(key, Json.fromInt(42).asRight)
    Thread.sleep(3000)
    cache.get(key) must beNone and (cache.actualLoad must beEqualTo(0))
  }

  def e3 = {
    val cache = Cache(2, 2)
    val key1 = ApiRequestEnrichment.cacheKey(url = "http://api.acme.com/url1", body = None)
    cache.put(key1, Json.fromInt(32).asRight)

    val key2 = ApiRequestEnrichment.cacheKey(url = "http://api.acme.com/url2", body = None)
    cache.put(key2, Json.fromInt(32).asRight)

    val key3 = ApiRequestEnrichment.cacheKey(url = "http://api.acme.com/url3", body = None)
    cache.put(key3, Json.fromInt(32).asRight)

    cache.get(key1) must beNone and (cache.actualLoad must beEqualTo(2))
  }

  def e4 = {
    val cache = Cache(3, 2)
    val key = ApiRequestEnrichment.cacheKey("http://api.acme.com/url", Some("""{"value":"42"}"""))

    cache.put(key, Json.fromInt(33).asRight)
    cache.put(key, Json.fromInt(42).asRight)
    cache.get(key) must beSome.like {
      case v => v must beRight(Json.fromInt(42))
    } and (cache.actualLoad must beEqualTo(1))
  }
}
