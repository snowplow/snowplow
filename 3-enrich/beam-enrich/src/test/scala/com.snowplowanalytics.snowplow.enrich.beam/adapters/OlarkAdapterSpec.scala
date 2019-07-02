/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam
package adapters

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._
import io.circe.literal._

object OlarkAdapterSpec {
  val body =
    "data=%7B%22kind%22%3A+%22Conversation%22%2C+%22tags%22%3A+%5B%22test_example%22%5D%2C+%22items%22%3A+%5B%7B%22body%22%3A+%22Hi+from+an+operator%22%2C+%22timestamp%22%3A+%221473774819.263083%22%2C+%22kind%22%3A+%22MessageToVisitor%22%2C+%22nickname%22%3A+%22Olark+operator%22%2C+%22operatorId%22%3A+%22647563%22%7D%2C+%7B%22body%22%3A+%22Hi+from+a+visitor%22%2C+%22timestamp%22%3A+%221473774821.411154%22%2C+%22kind%22%3A+%22MessageToOperator%22%2C+%22nickname%22%3A+%22Returning+Visitor+%7C+USA+%28San+Francisco%2C+CA%29+%237617%22%2C+%22visitorNickname%22%3A+%22Olark+Visitor%22%7D%5D%2C+%22operators%22%3A+%7B%22647563%22%3A+%7B%22username%22%3A+%22yali%22%2C+%22emailAddress%22%3A+%22yali%40snowplowanalytics.com%22%2C+%22kind%22%3A+%22Operator%22%2C+%22nickname%22%3A+%22Yali%22%2C+%22id%22%3A+%22647563%22%7D%7D%2C+%22visitor%22%3A+%7B%22city%22%3A+%22San+Francisco%22%2C+%22kind%22%3A+%22Visitor%22%2C+%22organization%22%3A+%22Visitor+Organization%22%2C+%22conversationBeginPage%22%3A+%22http%3A%2F%2Fwww.olark.com%22%2C+%22countryCode%22%3A+%22US%22%2C+%22referrer%22%3A+%22http%3A%2F%2Fwww.olark.com%22%2C+%22ip%22%3A+%22127.0.0.1%22%2C+%22region%22%3A+%22CA%22%2C+%22chat_feedback%22%3A+%7B%22overall_chat%22%3A+4%2C+%22responsiveness%22%3A+5%2C+%22friendliness%22%3A+5%2C+%22knowledge%22%3A+4%7D%2C+%22operatingSystem%22%3A+%22Windows%22%2C+%22emailAddress%22%3A+%22support%2Bintegrationtest%40olark.com%22%2C+%22country%22%3A+%22United+States%22%2C+%22phoneNumber%22%3A+%225555555555%22%2C+%22fullName%22%3A+%22Olark%22%2C+%22id%22%3A+%22NOTAREALVISITORIDS5LGl6QUrK2OaPP%22%2C+%22browser%22%3A+%22Internet+Explorer+11%22%7D%2C+%22id%22%3A+%22NOTAREALTRANSCRIPT5LGcbVTa3hKBRB%22%2C+%22manuallySubmitted%22%3A+false%7D"
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.olark/v1",
      body = body.some,
      contentType = "application/x-www-form-urlencoded".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.olark-v1",
    "event_vendor" -> "com.olark",
    "event_name" -> "transcript",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.olark/transcript/jsonschema/1-0-0","data":{"kind":"Conversation","tags":["test_example"],"items":[{"body":"Hi from an operator","timestamp":"2016-09-13T13:53:39.263Z","kind":"MessageToVisitor","nickname":"Olark operator","operatorId":"647563"},{"body":"Hi from a visitor","timestamp":"2016-09-13T13:53:41.411Z","kind":"MessageToOperator","nickname":"Returning Visitor | USA (San Francisco, CA) #7617","visitorNickname":"Olark Visitor"}],"operators":{"647563":{"username":"yali","emailAddress":"yali@snowplowanalytics.com","kind":"Operator","nickname":"Yali","id":"647563"}},"visitor":{"city":"San Francisco","kind":"Visitor","organization":"Visitor Organization","conversationBeginPage":"http://www.olark.com","countryCode":"US","referrer":"http://www.olark.com","ip":"127.0.0.1","region":"CA","chatFeedback":{"overallChat":4,"responsiveness":5,"friendliness":5,"knowledge":4},"operatingSystem":"Windows","emailAddress":"support+integrationtest@olark.com","country":"United States","phoneNumber":"5555555555","fullName":"Olark","id":"NOTAREALVISITORIDS5LGl6QUrK2OaPP","browser":"Internet Explorer 11"},"id":"NOTAREALTRANSCRIPT5LGcbVTa3hKBRB","manuallySubmitted":false}}}""".noSpaces
  )
}

class OlarkAdapterSpec extends PipelineSpec {
  import OlarkAdapterSpec._
  "OlarkAdapter" should "enrich using the olark adapter" in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI())
      )
      .input(PubsubIO[Array[Byte]]("in"), raw)
      .distCache(DistCacheIO(""), List.empty[Either[String, String]])
      .output(PubsubIO[String]("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO[String]("out")) { o =>
        o should satisfySingleValue { c: String =>
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
