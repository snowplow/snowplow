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
import cascading.tuple.{
  Fields,
  TupleEntry
}
import cascading.util.Util
import cascading.tap.partition.Partition

import com.twitter.scalding.{PartitionedTsv => StandardPartitionedTsv, _}

object PartitionSourceTestHelpers {
  import Dsl._

  class CustomPartition(val partitionFields: Fields) extends Partition {

    def getPartitionFields(): Fields = partitionFields
    def getPathDepth(): Int = 1
    
    def toPartition(tupleEntry: TupleEntry): String =
      "{" + Util.join(tupleEntry.asIterableOf(classOf[String]), "}->{", true) + "}"
    
    def toTuple(partition: String, tupleEntry: TupleEntry): Unit =
      throw new RuntimeException("toTuple for reading not implemented")
  }

  // Define once, here, otherwise testMode.getWritePathFor() won't work
  val DelimitedPartitionedTsv = StandardPartitionedTsv("base", "/", 'col1)
  val CustomPartitionedTsv = StandardPartitionedTsv("base", new CustomPartition('col1, 'col2), false, Fields.ALL, SinkMode.REPLACE)
  val PartialPartitionedTsv = StandardPartitionedTsv("base", new CustomPartition('col1, 'col2), false, ('col1, 'col2), SinkMode.REPLACE)
}

class DelimitedPartitionTestJob(args: Args) extends Job(args) {
  import PartitionSourceTestHelpers._
  try {
    Tsv("input", ('col1, 'col2)).read.write(DelimitedPartitionedTsv)
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class CustomPartitionTestJob(args: Args) extends Job(args) {
  import PartitionSourceTestHelpers._
  try {
    Tsv("input", ('col1, 'col2, 'col3)).read.write(CustomPartitionedTsv)
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class PartialPartitionTestJob(args: Args) extends Job(args) {
  import PartitionSourceTestHelpers._

  try {
    Tsv("input", ('col1, 'col2, 'col3)).read.write(PartialPartitionedTsv)
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class DelimitedPartitionSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import PartitionSourceTestHelpers._
  "PartitionedTsv fed a DelimitedPartition" should {
    "split output by the delimited path" in {
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

class CustomPartitionSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import PartitionSourceTestHelpers._
  "PartitionedTsv fed a CustomPartition" should {
    "split output by the custom path" in {
      val input = Seq(("A", "x", 1), ("A", "x", 2), ("B", "y", 3))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new CustomPartitionTestJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(Tsv("input", ('col1, 'col2, 'col3)), input)
        .runHadoop
        .finish

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(CustomPartitionedTsv))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("{A}->{x}", "{B}->{y}")

      val aSource = ScalaSource.fromFile(new File(directory, "{A}->{x}/part-00000-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "{B}->{y}/part-00000-00001"))

      aSource.getLines.toList mustEqual Seq("A\tx\t1", "A\tx\t2")
      bSource.getLines.toList mustEqual Seq("B\ty\t3")
    }
  }
}

class PartialPartitionSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import PartitionSourceTestHelpers._
  "PartitionedTsv fed a DelimitedPartition and only a subset of fields" should {
    "split output by the delimited path, discarding the unwanted fields" in {

      val input = Seq(("A", "x", 1), ("A", "x", 2), ("B", "y", 3))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new PartialPartitionTestJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(Tsv("input", ('col1, 'col2, 'col3)), input)
        .runHadoop
        .finish

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(PartialPartitionedTsv))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("A", "B")

      val aSource = ScalaSource.fromFile(new File(directory, "A/x/part-00000-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "B/y/part-00000-00001"))

      aSource.getLines.toList mustEqual Seq("A\t1", "A\t2")
      bSource.getLines.toList mustEqual Seq("B\t3")
    }
  }
}

