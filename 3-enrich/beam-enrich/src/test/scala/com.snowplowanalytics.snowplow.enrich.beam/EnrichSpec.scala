/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.Paths

import com.spotify.scio.testing._

class EnrichSpec extends PipelineSpec {

  val inData = Seq(Array[Byte](97, 98, 99))
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "Enrich" should "work" in {
    JobTest[Enrich.type]
      .args("--input=in", "--output=out", "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI()))
      .input(PubsubIO("in"), inData)
      .output(PubsubIO[String]("out"))(_ should containInAnyOrder (expected))
      .output(PubsubIO[String]("bad"))(_ should containInAnyOrder (expected))
      .run()
  }

}
