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

class ParseArgTypesTest extends Specification {

  // Aliases
  val refererUri = "http://www.psychicbazaar.com/catalog/pendula"
  val refererURI = new URI(refererUri)
  val pageURI = new URI("http://www.psychicbazaar.com/catalog/pendula/lo-scarabeo-silver-cone-pendulum")
  val pageHost = pageURI.getHost

  val expected = Some(Referer(Medium.Internal, None, None))

  "parse " should {
    "work the same regardless of which argument types are used to call it" in {
      Parser.parse(refererUri, pageHost) must_== expected
      Parser.parse(refererUri, pageURI)  must_== expected
      Parser.parse(refererURI, pageHost) must_== expected
      Parser.parse(refererURI, pageURI)  must_== expected
    }
  }
}