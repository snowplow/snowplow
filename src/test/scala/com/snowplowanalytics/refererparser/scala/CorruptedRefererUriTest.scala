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

class CorruptedRefererUriTest extends Specification {

  // Our data
  val refererUri = "http://bigcommerce%20wordpress%20plugin/"
  val pageUri = null.asInstanceOf[String]

  "A corrupted referer URI" should {
    "return None, not throw an Exception" in {
      Parser.parse(refererUri,pageUri) must beNone
    }
  }
}