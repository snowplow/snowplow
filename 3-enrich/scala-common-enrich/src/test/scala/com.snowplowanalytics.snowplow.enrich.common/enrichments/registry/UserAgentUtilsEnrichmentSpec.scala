 /**Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

class UserAgentUtilsEnrichmentSpec extends org.specs2.mutable.Specification with ValidationMatchers with DataTables {
  import UserAgentUtilsEnrichment._

  "useragent parser" should {
    "parse useragent" in {
      "SPEC NAME"   || "Input UserAgent"                                                                                                          | "Browser name"         | "Browser family"    | "Browser version"     | "Browser type" | "Browser rendering enging" | "OS fields"                                       | "Device type" | "Device is mobile" |
      "Safari spec" !! "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36" ! "Chrome 33"            ! "Chrome"            ! Some("33.0.1750.152") ! "Browser"      ! "WEBKIT"                   ! ("Mac OS X", "Mac OS X", "Apple Inc.")            ! "Computer"    ! false              |
      "IE spec"     !! "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0"                                                                 ! "Internet Explorer 11" ! "Internet Explorer" ! Some("11.0")          ! "Browser"      ! "TRIDENT"                  ! ("Windows 7", "Windows", "Microsoft Corporation") ! "Computer"    ! false              |> {

        (_, input, browserName, browserFamily, browserVersion, browserType, browserRenderEngine, osFields, deviceType, deviceIsMobile) => {
          val expected = ClientAttributes(browserName, browserFamily, browserVersion, browserType, browserRenderEngine, osFields._1, osFields._2, osFields._3, deviceType, deviceIsMobile)
          UserAgentUtilsEnrichment.extractClientAttributes(input) must beSuccessful(expected)
        }
      }
    }
  }
}
