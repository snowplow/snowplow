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

import scala.io.Source

import cats.syntax.option._
import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing._

object Tp2MegaEventsAdapterSpec {
  val body = Source.fromResource("mega-events.txt").getLines().mkString
  val raw = Seq(
    SpecHelpers.buildCollectorPayload(
      path = "/com.snowplowanalytics.snowplow/tp2",
      body = body.some,
      contentType = "application/json".some
    )
  )
}

class Tp2MegaEventsAdapterSpec extends PipelineSpec {
  import Tp2MegaEventsAdapterSpec._
  "Tp2Adapter" should "enrich mega events using the tp2 adapter" in {
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
        o should haveSize(7500); ()
      }
      .run()
  }
}
