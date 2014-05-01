/*
Copyright 2014 Snowplow Analytics Ltd

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding

import java.io.File
import scala.io.{Source => ScalaSource}

import org.specs._

import cascading.tap.SinkMode
import cascading.tuple.Fields

import com.twitter.scalding.{PartitionedTsv => StandardPartitionedTsv, _}

object PartitionSourceTestHelpers {
  import Dsl._
  // Define once otherwise testMode.getWritePathFor() won't work
  val DelimitedPartitionedTsv = StandardPartitionedTsv("base", "/", 'col1)
}

class DelimitedPartitionTestJob(args: Args) extends Job(args) {
  import PartitionSourceTestHelpers._
  try {
    Tsv("input", ('col1, 'col2)).read.write(DelimitedPartitionedTsv)
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class DelimitedPartitionSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import PartitionSourceTestHelpers._
  "PartitionedTsv" should {
    "split output by partition" in {
      val input = Seq(("A", 1), ("A", 2), ("B", 3))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new DelimitedPartitionTestJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(Tsv("input", ('col1, 'col2)), input)
        .runHadoop
        .finish

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(DelimitedPartitionedTsv))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("A", "B")

      val aSource = ScalaSource.fromFile(new File(directory, "A/part-00000-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "B/part-00000-00001"))

      aSource.getLines.toList mustEqual Seq("A\t1", "A\t2")
      bSource.getLines.toList mustEqual Seq("B\t3")
    }
  }
}
