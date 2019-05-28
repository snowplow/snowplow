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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import cats.syntax.option._
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

class TsvLoaderSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the TsvLoader functionality
  toCollectorPayload should return a CollectorPayload for a normal TSV                                      $e1
  toCollectorPayload should return None for the first two lines of a Cloudfront web distribution access log $e2
  """

  def e1 = {
    val expected = CollectorPayload(
      api = CollectorApi("com.amazon.aws.cloudfront", "wd_access_log"),
      querystring = Nil,
      body = "a\tb".some,
      contentType = None,
      source = CollectorSource("tsv", "UTF-8", None),
      context = CollectorContext(None, None, None, None, Nil, None)
    )
    TsvLoader("com.amazon.aws.cloudfront/wd_access_log").toCP("a\tb") must beValid(
      expected.some
    )
  }

  def e2 =
    TsvLoader("com.amazon.aws.cloudfront/wd_access_log").toCP("#Version: 1.0") must beValid(
      None
    )
}
