 /**Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package registry

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz._

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class UaParserEnrichmentSpec extends org.specs2.mutable.Specification with ValidationMatchers with DataTables {
  import UaParserEnrichment._
  val mobileSafariJson = """{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Mobile Safari","useragentMajor":"5","useragentMinor":"1","useragentPatch":null,"useragentVersion":"Mobile Safari 5.1","osFamily":"iOS","osMajor":"5","osMinor":"1","osPatch":"1","osPatchMinor":null,"osVersion":"iOS 5.1.1","deviceFamily":"iPhone"}}"""
  val safariJson = """{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Safari","useragentMajor":"8","useragentMinor":"0","useragentPatch":null,"useragentVersion":"Safari 8.0","osFamily":"Mac OS X","osMajor":"10","osMinor":"10","osPatch":null,"osPatchMinor":null,"osVersion":"Mac OS X 10.10","deviceFamily":"Other"}}"""

  "useragent parser" should {
    "parse useragent" in {
      "Input UserAgent" | "Parsed UserAgent" |
      "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3" !! mobileSafariJson |
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25" !! safariJson |> {
        (input, expected) => {
          UaParserEnrichment.extractUserAgent(input) must beSuccessful.like { case a => compact(render(a)) must_== compact(render(parse(expected))) }
        }
      }
    }
  }
}
