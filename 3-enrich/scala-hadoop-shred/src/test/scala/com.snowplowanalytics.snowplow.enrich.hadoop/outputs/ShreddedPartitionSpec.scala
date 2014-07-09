/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package outputs

// Java
import java.io.File

// Cascading
import cascading.tap.SinkMode

// Scala
import scala.io.{Source => ScalaSource}

// Scalding
import com.twitter.scalding.{PartitionedTsv => StandardPartitionedTsv, _}

// Specs2
import org.specs2.mutable.Specification

object PartitionSourceTestHelpers {
  import Dsl._

  // Define once, here, otherwise testMode.getWritePathFor() won't work
  val ShreddedPartitionedTsv = StandardPartitionedTsv("base", new ShreddedPartition('schema), false, ('instance), SinkMode.REPLACE)
}

class ShreddedPartitionTestJob(args: Args) extends Job(args) {
  import PartitionSourceTestHelpers._
  try {
    Tsv("input", ('schema, 'instance)).read.write(ShreddedPartitionedTsv)
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class CustomPartitionSourceTest extends Specification {

  import Dsl._
  import PartitionSourceTestHelpers._
  "PartitionedTsv fed a ShreddedPartition" should {
    "split output by the schema path" in {
      val input = Seq(
        ("iglu:com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0", """{ "event": "ad_click" } """),
        ("iglu:com.zendesk/new-ticket/jsonschema/1-1-0", """{ "event": "new-ticket" } """)
        )

      // Need to save the job to allow us to find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new ShreddedPartitionTestJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(Tsv("input", ('schema, 'instance)), input)
        .runHadoop
        .finish

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(ShreddedPartitionedTsv))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("com.snowplowanalytics.snowplow", "com.zendesk")

      val adClickSource = ScalaSource.fromFile(new File(directory, "com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0/part-00000-00000"))
      val newTicketSource = ScalaSource.fromFile(new File(directory, "com.zendesk/new-ticket/jsonschema/1-1-0/part-00000-00001"))

      adClickSource.getLines.toList mustEqual Seq("""{ "event": "ad_click" } """)
      newTicketSource.getLines.toList mustEqual Seq("""{ "event": "new-ticket" } """)
    }
  }
}
