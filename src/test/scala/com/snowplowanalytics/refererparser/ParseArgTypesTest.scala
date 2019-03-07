/**
 * Copyright 2012-2019 Snowplow Analytics Ltd
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
package com.snowplowanalytics.refererparser

import java.net.URI

import cats.effect.IO
import org.specs2.mutable.Specification

class ParseArgTypesTest extends Specification {

  val resource   = getClass.getResource("/referers.json").getPath
  val ioParser   = Parser.create[IO](resource).unsafeRunSync().fold(throw _, identity)
  val evalParser = Parser.unsafeCreate(resource).value.fold(throw _, identity)

  "parse " should {
    "work the same regardless of which argument types are used to call it" in {
      val refererUri = "http://www.psychicbazaar.com/catalog/pendula"
      val refererURI = new URI(refererUri)
      val pageURI =
        new URI("http://www.psychicbazaar.com/catalog/pendula/lo-scarabeo-silver-cone-pendulum")
      val pageHost = pageURI.getHost
      val expected = Some(InternalReferer)
      ioParser.parse(refererUri, pageHost) must_== expected
      ioParser.parse(refererUri, pageURI) must_== expected
      ioParser.parse(refererURI, pageHost) must_== expected
      ioParser.parse(refererURI, pageURI) must_== expected
      evalParser.parse(refererUri, pageHost) must_== expected
      evalParser.parse(refererUri, pageURI) must_== expected
      evalParser.parse(refererURI, pageHost) must_== expected
      evalParser.parse(refererURI, pageURI) must_== expected
    }
  }
}
