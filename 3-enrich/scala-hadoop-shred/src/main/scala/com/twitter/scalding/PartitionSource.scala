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
import cascading.tap.partition.DelimitedPartition
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields

/**
* This is a base class for partition-based output sources
*/
abstract class PartitionSource extends SchemedSource {

  // The root path of the partitioned output.
  def basePath: String
  // The path delimiter for creating (sub-)directory bins.
  def delimiter: String = "/"
  // The fields to apply to the partition.
  // ALL doesn't make sense as partitioned fields are discarded from output by default (see below)
  def pathFields: Fields = Fields.FIRST
  // Whether to remove path fields prior to writing. 
  def discardPathFields: Boolean = false

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
            // TODO add support for discardPathFields  
            val localTap = new FileTap(localScheme, basePath, sinkMode)
            val partition = new DelimitedPartition(pathFields, delimiter)
            new LPartitionTap(localTap, partition)
          }
          case hdfsMode @ Hdfs(_, _) => {
            // TODO add support for discardPathFields 
            val hfsTap = new Hfs(hdfsScheme, basePath, sinkMode)
            val partition = new DelimitedPartition(pathFields, delimiter)
            new HPartitionTap(hfsTap, partition)
          }
          case hdfsTest @ HadoopTest(_, _) => {
            // Doesn't work
            if (discardPathFields) {
              hdfsScheme.setSinkFields(hdfsScheme.getSinkFields.subtract(pathFields))
            }

            val hfsTap = new Hfs(hdfsScheme, hdfsTest.getWritePathFor(this), sinkMode)
            val partition = new DelimitedPartition(pathFields, delimiter)
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
      throw new InvalidSourceException("basePath cannot be null for TemplateTap")
    }
  }
}

/**
 * An implementation of TSV output, split over a partition tap.
 *
 * @param basePath The root path for the output.
 * @param delimiter The path delimiter, defaults to / to create sub-directory bins.
 * @param pathFields The set of fields to apply to the path.
 * @param discardPathFields Whether to remove path fields prior to writing.
 * @param writeHeader Flag to indicate that the header should be written to the file.
 * @param sinkMode How to handle conflicts with existing output.
 */
case class PartitionedTsv(
  override val basePath: String,
  override val delimiter: String = "/",
  override val pathFields: Fields = Fields.FIRST,
  override val discardPathFields: Boolean = false,
  override val writeHeader: Boolean = false,
  override val sinkMode: SinkMode = SinkMode.REPLACE)
    extends PartitionSource with DelimitedScheme

/**
 * An implementation of SequenceFile output, split over a partition tap.
 *
 * @param basePath The root path for the output.
 * @param delimiter The path delimiter, defaults to / to create sub-directory bins.
 * @param sequenceFields The set of fields to use for the sequence file.
 * @param pathFields The set of fields to apply to the path.
 * @param discardPathFields Whether to remove path fields prior to writing.
 * @param sinkMode How to handle conflicts with existing output.
 */
case class PartitionedSequenceFile(
  override val basePath: String,
  override val delimiter: String = "/",
  val sequenceFields: Fields = Fields.ALL,
  override val pathFields: Fields = Fields.FIRST,
  override val discardPathFields: Boolean = false,
  override val sinkMode: SinkMode = SinkMode.REPLACE)
    extends PartitionSource with SequenceFileScheme {

  override val fields = sequenceFields
}
