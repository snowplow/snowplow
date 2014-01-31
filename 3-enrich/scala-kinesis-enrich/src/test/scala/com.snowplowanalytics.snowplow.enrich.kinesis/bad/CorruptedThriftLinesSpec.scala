/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package enrich.kinesis
package bad

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.{Specification, ScalaCheck}

// ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary._

class CorruptedThrift extends Specification with ScalaCheck { def is =

  "This is a specification to test handling of corrupted Thrift payloads"                                   ^
                                                                                                           p^
  "Scala Kinesis Enrich should return None for a corrupted Thrift raw event"                                ! e1^
                                                                                                            end

  def e1 = check {
    (raw: String) => {
      val eventBytes = Base64.decodeBase64(raw)
      SpecHelpers.testSource.enrichEvent(eventBytes) must beNone
    }
  }

}
