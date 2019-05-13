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

import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables

class CollectorPayloadSpec extends Specification with DataTables {

  // TODO: let's abstract this up to a CollectorApi.parse test
  // (then we can make isIceRequest private again).
  "isIceRequest" should {
    "correctly identify valid Snowplow GET requests" in {
      "SPEC NAME" || "PATH" | "EXP. RESULT" |
        "Valid #1" !! "/i" ! true |
        "Valid #2" !! "/ice.png" ! true |
        "Valid #3" !! "/i?foo=1&bar=2" ! true |
        "Invalid #1" !! "/blah/i" ! false |
        "Invalid #2" !! "i" ! false |> { (_, path, expected) =>
        CollectorPayload.isIceRequest(path) must_== expected
      }
    }
  }
}
