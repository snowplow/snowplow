/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowplowanalytics.refererparser.scala

// Java
import java.net.URI

// Specs2
import org.specs2.mutable.Specification

class NoRefererUriTest extends Specification {

  // Our data
  val pageHost = "www.psychicbazaar.com"

  "An empty referer URI" should {
    "return no referal" in {
      Parser.parse("", pageHost) must beNone
    }
  }

  "A null [String] referer URI" should {
    "return no referal" in {
      Parser.parse(null.asInstanceOf[String], pageHost) must beNone
    }
  }

  "A null [URI] referer URI" should {
    "return no referal" in {
      Parser.parse(null.asInstanceOf[URI], pageHost) must beNone
    }
  }
}