/**
 * Copyright 2012-2018 Snowplow Analytics Ltd
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

// Java
import java.net.URI

import cats.effect.IO
import org.specs2.mutable.Specification

class ParseArgTypesTest extends Specification {

  // Aliases
  val refererUri = "http://www.psychicbazaar.com/catalog/pendula"
  val refererURI = new URI(refererUri)
  val pageURI = new URI(
    "http://www.psychicbazaar.com/catalog/pendula/lo-scarabeo-silver-cone-pendulum")
  val pageHost = pageURI.getHost

  val expected = Some(InternalReferer)

  val parser = Parser
    .create[IO](
      getClass.getResource("/referers.json").getPath
    )
    .unsafeRunSync() match {
    case Right(p) => p
    case Left(f)  => throw f
  }

  "parse " should {
    "work the same regardless of which argument types are used to call it" in {
      parser.parse(refererUri, pageHost) must_== expected
      parser.parse(refererUri, pageURI) must_== expected
      parser.parse(refererURI, pageHost) must_== expected
      parser.parse(refererURI, pageURI) must_== expected
    }
  }
}
