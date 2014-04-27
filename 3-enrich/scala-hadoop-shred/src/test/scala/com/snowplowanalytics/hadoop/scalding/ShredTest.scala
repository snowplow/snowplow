/*
Copyright 2013 Inkling, Inc.

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
package com.snowplowanalytics.hadoop.scalding

import java.io.File
import scala.io.{Source => ScalaSource}

// Specs2
import org.specs2.mutable.Specification

import cascading.tap.SinkMode
import cascading.tuple.Fields

// Scalding
import com.twitter.scalding._

class TemplateTestJob(args: Args) extends Job(args) {
  try {
    Tsv("input", ('col1, 'col2, 'col3)).read.write(TemplatedTsv("base", "%s/%s", ('col1, 'col2)))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class TemplateSourceTest extends Specification {

  import Dsl._
  "TemplatedTsv" should {
    "split output by template" in {
      val input = Seq(("A", "a", 1), ("A", "a", 2), ("B", "b", 3))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new TemplateTestJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(Tsv("input", ('col1, 'col2, 'col3)), input)
        .runHadoop
        .finish

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(TemplatedTsv("base", "%s/%s", ('col1, 'col2))))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("A", "B")

      val aSource = ScalaSource.fromFile(new File(directory, "A/a/part-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "B/b/part-00000"))

      aSource.getLines.toList mustEqual Seq("A\ta\t1", "A\ta\t2")
      bSource.getLines.toList mustEqual Seq("B\tb\t3")
    }
  }
}
