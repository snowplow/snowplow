/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package good

import scala.io.Source

import org.specs2.mutable.Specification

object CljTomcatTp2MegaEventsSpec {
  val lines = {
    val file = getClass.getResource("CljTomcatTp2MegaEventsSpec.line").getFile
    val source = Source.fromFile(file)
    val line = source.mkString
    source.close()
    EnrichJobSpec.Lines(line)
  }
}

class CljTomcatTp2MegaEventsSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-tp2-mega-events"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing 7,500 " +
  "valid events" should {
    runEnrichJob(CljTomcatTp2MegaEventsSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 7,500 events" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 7500
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
