/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream
package bad

import org.apache.commons.codec.binary.Base64
import org.scalacheck.Arbitrary._
import org.specs2.{ScalaCheck, Specification}

import SpecHelpers._

class CorruptedThriftLinesSpec extends Specification with ScalaCheck {
  def is =
    "This is a specification to test handling of corrupted Thrift payloads" ^
      p ^
      "Stream Enrich should return None for any corrupted Thrift raw events" ! e1 ^
      end

  // A bit of fun: the chances of generating a valid Thrift SnowplowRawEvent at random are
  // so low that we can just use ScalaCheck here
  def e1 = check { (raw: String) =>
    {
      val eventBytes = Base64.decodeBase64(raw)
      TestSource.enrichEvents(eventBytes)(0).isFailure must beTrue
    }
  }

}
