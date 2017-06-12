/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments
package registry
package apirequest

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JInt

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.mock.Mockito

class CacheSpec extends Specification with ValidationMatchers with Mockito { def is = s2"""
  This is a specification to test the API Request enrichment cache
  Update on identical URLs $e1
  Preserve ttl of cache    $e2
  Remove unused value      $e3
  """

  def e1 = {
    val cache = Cache(3, 2)
    cache.put("http://api.acme.com/url", JInt(42).success)
    cache.put("http://api.acme.com/url", JInt(52).success)
    cache.get("http://api.acme.com/url") must beSome.like {
      case v => v must beSuccessful(JInt(52))
    } and(cache.actualLoad must beEqualTo(1))
  }

  def e2 = {
    val cache = Cache(3, 2)
    cache.put("http://api.acme.com/url", JInt(42).success)
    Thread.sleep(3000)
    cache.get("http://api.acme.com/url") must beNone and(cache.actualLoad must beEqualTo(0))
  }

  def e3 = {
    val cache = Cache(2, 2)
    cache.put("http://api.acme.com/url1", JInt(32).success)
    cache.put("http://api.acme.com/url2", JInt(42).success)
    cache.put("http://api.acme.com/url3", JInt(52).success)
    cache.get("http://api.acme.com/url1") must beNone and(cache.actualLoad must beEqualTo(2))
  }
}
