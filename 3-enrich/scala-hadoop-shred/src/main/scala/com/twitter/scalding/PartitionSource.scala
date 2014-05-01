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

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector

import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.hadoop.TextLine.Compress
import cascading.scheme.Scheme
import cascading.tap.hadoop.Hfs
import cascading.tap.hadoop.{ PartitionTap => HPartitionTap }
import cascading.tap.local.FileTap
import cascading.tap.local.{ PartitionTap => LPartitionTap }
import cascading.tap.partition.{
  DelimitedPartition,
  Partition
}
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields

/**
* This is a base class for partition-based output sources
*/
abstract class PartitionSource extends SchemedSource {

  // The root path of the partitioned output.
  def basePath: String
  // The partition.
  def partition: Partition = new DelimitedPartition(Fields.ALL, "/")

  /**
   * Creates the partition tap.
   *
   * @param readOrWrite Describes if this source is being read from or written to.
   * @param mode The mode of the job. (implicit)
   *
   * @returns A cascading PartitionTap.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    readOrWrite match {
      case Read => throw new InvalidSourceException("Using PartitionSource for input not yet implemented")
      case Write => {
        mode match {
          case Local(_) => {
            val localTap = new FileTap(localScheme, basePath, sinkMode)
            new LPartitionTap(localTap, partition)
          }
          case hdfsMode @ Hdfs(_, _) => {
            val hfsTap = new Hfs(hdfsScheme, basePath, sinkMode)
            new HPartitionTap(hfsTap, partition)
          }
          case hdfsTest @ HadoopTest(_, _) => {
            val hfsTap = new Hfs(hdfsScheme, hdfsTest.getWritePathFor(this), sinkMode)
            new HPartitionTap(hfsTap, partition)
          }
          case _ => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
        }
      }
    }
  }

  /**
   * Validates the taps, makes sure there are no nulls in the path.
   *
   * @param mode The mode of the job.
   */
  override def validateTaps(mode: Mode): Unit = {
    if (basePath == null) {
      throw new InvalidSourceException("basePath cannot be null for PartitionTap")
    }
  }
}

/**
 * An implementation of TSV output, split over a partition tap.
 *
 * apply assumes user wants a DelimitedPartition (the only
 * strategy bundled with Cascading).
 *
 * @param basePath The root path for the output.
 * @param delimiter The path delimiter, defaults to / to create sub-directory bins.
 * @param pathFields The set of fields to apply to the path.
 * @param writeHeader Flag to indicate that the header should be written to the file.
 * @param sinkMode How to handle conflicts with existing output.
 */
object PartitionedTsv {
  def apply(
    basePath: String,
    delimiter: String = "/",
    pathFields: Fields = Fields.ALL,
    writeHeader: Boolean = false,
    sinkMode: SinkMode = SinkMode.REPLACE
  ) = new PartitionedTsv(basePath, new DelimitedPartition(pathFields, delimiter), writeHeader, sinkMode)
}

/**
 * An implementation of TSV output, split over a partition tap.
 *
 * @param basePath The root path for the output.
 * @param partition The partitioning strategy to use.
 * @param writeHeader Flag to indicate that the header should be written to the file.
 * @param sinkMode How to handle conflicts with existing output.
 */
case class PartitionedTsv(
  override val basePath: String,
  override val partition: Partition,
  override val writeHeader: Boolean,
  override val sinkMode: SinkMode)
    extends PartitionSource with DelimitedScheme

/**
 * An implementation of SequenceFile output, split over a partition tap.
 *
 * apply assumes user wants a DelimitedPartition (the only
 * strategy bundled with Cascading).
 *
 * @param basePath The root path for the output.
 * @param delimiter The path delimiter, defaults to / to create sub-directory bins.
 * @param pathFields The set of fields to apply to the path.
 * @param sequenceFields The set of fields to use for the sequence file.
 * @param sinkMode How to handle conflicts with existing output.
 */
object PartitionedSequenceFile {
  def apply(
    basePath: String,
    delimiter: String = "/",
    pathFields: Fields = Fields.ALL,
    sequenceFields: Fields = Fields.ALL,
    sinkMode: SinkMode = SinkMode.REPLACE
  ) = new PartitionedSequenceFile(basePath, new DelimitedPartition(pathFields, delimiter), sequenceFields, sinkMode)
}

/**
 * An implementation of SequenceFile output, split over a partition tap.
 *
 * @param basePath The root path for the output.
 * @param partition The partitioning strategy to use.
 * @param sequenceFields The set of fields to use for the sequence file.
 * @param sinkMode How to handle conflicts with existing output.
 */
case class PartitionedSequenceFile(
  override val basePath: String,
  override val partition: Partition,
  val sequenceFields: Fields,
  override val sinkMode: SinkMode)
    extends PartitionSource with SequenceFileScheme {

  override val fields = sequenceFields
}
