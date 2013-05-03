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

class NoPageUriTest extends Specification {

  // Our data
  val refererUri = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
  val expected   = Some(Referer(Medium.Search, Some("Google"), Some("gateway oracle cards denise linn")))

  "An empty page URI" should {
    "not interfere with the referer parsing" in {
      Parser.parse(refererUri, "") must_== expected
    }
  }

  "A null (String) page URI" should {
    "not interfere with the referer parsing" in {
      Parser.parse(refererUri, null.asInstanceOf[String]) must_== expected
    }
  }

  "A null (URI) page URI" should {
    "not interfere with the referer parsing" in {
      Parser.parse(refererUri, null.asInstanceOf[URI]) must_== expected
    }
  }
}