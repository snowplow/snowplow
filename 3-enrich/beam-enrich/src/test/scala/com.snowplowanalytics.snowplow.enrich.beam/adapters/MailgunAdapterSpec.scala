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

object MailgunAdapterSpec {
  val body =
    "domain=sandbox57070072075d4cfd9008d4332108734c.mailgun.org&my_var_1=Mailgun+Variable+%231&my-var-2=awesome&message-headers=%5B%5B%22Received%22%2C+%22by+luna.mailgun.net+with+SMTP+mgrt+8734663311733%3B+Fri%2C+03+May+2013+18%3A26%3A27+%2B0000%22%5D%2C+%5B%22Content-Type%22%2C+%5B%22multipart%2Falternative%22%2C+%7B%22boundary%22%3A+%22eb663d73ae0a4d6c9153cc0aec8b7520%22%7D%5D%5D%2C+%5B%22Mime-Version%22%2C+%221.0%22%5D%2C+%5B%22Subject%22%2C+%22Test+deliver+webhook%22%5D%2C+%5B%22From%22%2C+%22Bob+%3Cbob%40sandbox57070072075d4cfd9008d4332108734c.mailgun.org%3E%22%5D%2C+%5B%22To%22%2C+%22Alice+%3Calice%40example.com%3E%22%5D%2C+%5B%22Message-Id%22%2C+%22%3C20130503182626.18666.16540%40sandbox57070072075d4cfd9008d4332108734c.mailgun.org%3E%22%5D%2C+%5B%22X-Mailgun-Variables%22%2C+%22%7B%5C%22my_var_1%5C%22%3A+%5C%22Mailgun+Variable+%231%5C%22%2C+%5C%22my-var-2%5C%22%3A+%5C%22awesome%5C%22%7D%22%5D%2C+%5B%22Date%22%2C+%22Fri%2C+03+May+2013+18%3A26%3A27+%2B0000%22%5D%2C+%5B%22Sender%22%2C+%22bob%40sandbox57070072075d4cfd9008d4332108734c.mailgun.org%22%5D%5D&Message-Id=%3C20130503182626.18666.16540%40sandbox57070072075d4cfd9008d4332108734c.mailgun.org%3E&recipient=alice%40example.com&event=delivered&timestamp=1510161827&token=cd87f5a30002794e37aa49e67fb46990e578b1e9197773d817&signature=c902ff9e3dea54c2dbe1871f9041653292ea9689d3d2b2d2ecfa996f025b9669&body-plain="
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.mailgun/v1",
      body = body.some,
      contentType = "application/x-www-form-urlencoded".some
    )
  )
  val expected = Map(
    "v_tracker" -> "com.mailgun-v1",
    "event_vendor" -> "com.mailgun",
    "event_name" -> "message_delivered",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailgun/message_delivered/jsonschema/1-0-0","data":{"recipient":"alice@example.com","timestamp":"2017-11-08T17:23:47.000Z","domain":"sandbox57070072075d4cfd9008d4332108734c.mailgun.org","signature":"c902ff9e3dea54c2dbe1871f9041653292ea9689d3d2b2d2ecfa996f025b9669","messageHeaders":"[[\"Received\", \"by luna.mailgun.net with SMTP mgrt 8734663311733; Fri, 03 May 2013 18:26:27 +0000\"], [\"Content-Type\", [\"multipart/alternative\", {\"boundary\": \"eb663d73ae0a4d6c9153cc0aec8b7520\"}]], [\"Mime-Version\", \"1.0\"], [\"Subject\", \"Test deliver webhook\"], [\"From\", \"Bob <bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"To\", \"Alice <alice@example.com>\"], [\"Message-Id\", \"<20130503182626.18666.16540@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>\"], [\"X-Mailgun-Variables\", \"{\\\"my_var_1\\\": \\\"Mailgun Variable #1\\\", \\\"my-var-2\\\": \\\"awesome\\\"}\"], [\"Date\", \"Fri, 03 May 2013 18:26:27 +0000\"], [\"Sender\", \"bob@sandbox57070072075d4cfd9008d4332108734c.mailgun.org\"]]","myVar1":"Mailgun Variable #1","token":"cd87f5a30002794e37aa49e67fb46990e578b1e9197773d817","messageId":"<20130503182626.18666.16540@sandbox57070072075d4cfd9008d4332108734c.mailgun.org>","myVar2":"awesome"}}}""".noSpaces
  )
}

class MailgunAdapterSpec extends PipelineSpec {
  import MailgunAdapterSpec._
  "MailgunAdapter" should "enrich using the mailgun adapter" in {
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
          println(SpecHelpers.buildEnrichedEvent(c))
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
