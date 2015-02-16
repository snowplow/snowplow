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
package com.snowplowanalytics.snowplow
package enrich
package common
package loaders

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary._

class TsvLoaderSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the TsvLoader functionality"                                                      ^
                                                                                                                    p^
  "toCollectorPayload should return a CollectorPayload for a normal TSV"                                             ! e1^
  "toCollectorPayload should return None for the first two lines of a Cloudfront web distribution access log"        ! e2^
                                                                                                                     end

  def e1 = {
    val expected = CollectorPayload(
          api          = CollectorApi("com.amazon.aws.cloudfront", "wd_access_log"),
          querystring  = Nil,
          body         = "a\tb".some,
          contentType  = None,
          source       = CollectorSource("tsv", "UTF-8", None),
          context      = CollectorContext(None, None, None, None, Nil, None)
          )
    TsvLoader("com.amazon.aws.cloudfront/wd_access_log").toCollectorPayload("a\tb") must beSuccessful(expected.some)
  }

  def e2 = TsvLoader("com.amazon.aws.cloudfront/wd_access_log").toCollectorPayload("#Version: 1.0") must beSuccessful(None)
}
