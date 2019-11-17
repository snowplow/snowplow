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
package com.snowplowanalytics.snowplow.enrich
package spark

// Java
import java.net.URI
import java.nio.file.{Files, Paths}

import cats.data.ValidatedNel

import cats.data.{NonEmptyList, Validated}

// Spark
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Elephant Bird
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat
import com.twitter.elephantbird.mapreduce.io.BinaryWritable

// Hadoop
import org.apache.hadoop.io.LongWritable

// Hadop LZO
import com.hadoop.compression.lzo.{LzoCodec, LzopCodec}

// Snowplow
import common.{EtlPipeline, FatalEtlError}
import common.adapters.AdapterRegistry
import common.loaders.Loader
import common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import EnrichJobConfig.ParsedEnrichJobConfig

object EnrichJob {
  // Classes that need registering for Kryo
  private[spark] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Array[Array[Byte]]],
    classOf[EnrichedEvent],
    classOf[Array[EnrichedEvent]],
    classOf[com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload],
    classOf[Array[com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload]],
    Class.forName(
      "com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload$_Fields"),
    Class.forName(
      "[Lcom.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload$_Fields;"),
    classOf[com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent],
    classOf[Array[com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent]],
    Class.forName("com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent$_Fields"),
    Class.forName("[Lcom.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent$_Fields;"),
    classOf[com.snowplowanalytics.snowplow.collectors.thrift.TrackerPayload],
    classOf[com.snowplowanalytics.snowplow.collectors.thrift.PayloadFormat],
    classOf[com.snowplowanalytics.snowplow.collectors.thrift.PayloadProtocol],
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
    Class.forName("scala.collection.convert.Wrappers$")
  )

  def apply(spark: SparkSession, args: Array[String]) = new EnrichJob(spark, args)

  val processor = Processor("spark", generated.BuildInfo.version)

  type ValidatedEnrichedEvent = ValidatedNel[BadRow, EnrichedEvent]

  /**
   * Project our Failures into a List of Nel of strings.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of failures, where each failure is a Nel of strings
   */
  def projectBads(all: List[ValidatedEnrichedEvent]): List[NonEmptyList[BadRow]] =
    all.collect { case Validated.Invalid(errs) => errs }

  /**
   * Project our Sucesses into EnrichedEvents.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of EnrichedEvent
   */
  def projectGoods(all: List[ValidatedEnrichedEvent]): List[EnrichedEvent] =
    all.collect { case Validated.Valid(e) => e }

  /**
   * Turn a raw line into an EnrichedEvent.
   * @param line String or Array[Byte] representing a raw event
   * @param config config for the job
   * @return the input raw line as well as the EnrichedEvent boxed in a Validation
   */
  def enrich(line: Any, config: ParsedEnrichJobConfig): List[ValidatedEnrichedEvent] = {
    import singleton._
    val adapterRegistry = new AdapterRegistry
    val enrichmentRegistry =
      RegistrySingleton.get(config.igluConfig, config.enrichments, config.local)
    val loader = LoaderSingleton.get(config.inFormat).asInstanceOf[Loader[Any]]
    val input  = loader.toCollectorPayload(line, processor)
    EtlPipeline.processEvents(
      adapterRegistry,
      enrichmentRegistry,
      ResolverSingleton.get(config.igluConfig),
      processor,
      config.etlTstamp,
      input)
  }

  /**
   * Create a symbolic link for each cached file.
   * @param fileNamesToCache list of filenames and associated symbolic links that need caching
   */
  def createSymbolicLinks(fileNamesToCache: List[(String, String)]): Unit =
    for ((filename, symlink) <- fileNamesToCache) {
      val symlinkPath = Paths.get(symlink)
      if (Files.notExists(symlinkPath))
        Files.createSymbolicLink(symlinkPath, Paths.get(SparkFiles.get(filename)))
    }

  /**
   * A helper to get the filename from a URI.
   * @param uri The URL to extract the filename from
   * @return The extracted filename
   */
  def extractFilenameFromURI(uri: URI): String = {
    val p = uri.getPath
    p.substring(p.lastIndexOf('/') + 1, p.length)
  }
}

/**
 * The Snowplow Enrich job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param args Command line arguments for the enrich job
 */
class EnrichJob(@transient val spark: SparkSession, args: Array[String]) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext

  // Necessary to be able to deal with LZO-compressed files
  @transient private val hadoopConfig = sc.hadoopConfiguration
  hadoopConfig.set("io.compression.codecs", classOf[LzopCodec].getName())
  hadoopConfig.set("io.compression.codec.lzo.class", classOf[LzoCodec].getName())

  // Job configuration
  private val enrichConfig = EnrichJobConfig
    .loadConfigFrom(args)
    .fold(
      e => throw FatalEtlError(e.map(_.toString)),
      identity
    )

  /**
   * Run the enrich job by:
   *   - reading input events in different collector formats
   *   - enriching them
   *   - writing out properly-formed and malformed events
   */
  def run(): Unit = {
    import EnrichJob._

    // Install MaxMind file(s) if we have them
    for ((uri, _) <- enrichConfig.filesToCache) { sc.addFile(uri.toString) }
    val fileNamesToCache = enrichConfig.filesToCache
      .map { case (uri, sl) => (extractFilenameFromURI(uri), sl) }

    val input = getInputRDD(enrichConfig.inFormat, enrichConfig.inFolder)
      .map { e =>
        // no-op map creating the symlinks
        createSymbolicLinks(fileNamesToCache)
        e
      }

    val common = input
      .map(enrich(_, enrichConfig))
      .cache()

    // Handling of malformed rows
    val bad = common
      .flatMap { enriched =>
        projectBads(enriched).flatMap(_.toList).map(e => Row(e.compact))
      }

    spark
      .createDataFrame(bad, StructType(StructField("_", StringType, true) :: Nil))
      .write
      .mode(SaveMode.Overwrite)
      .text(enrichConfig.badFolder)

    // Handling of properly-formed rows
    val good = common.flatMap(projectGoods)

    spark
      .createDataset(good)(Encoders.bean(classOf[EnrichedEvent]))
      .toDF()
      // hack to preserve the order of the fields in the csv, otherwise it's alphabetical
      .select(
        classOf[EnrichedEvent]
          .getDeclaredFields()
          .filterNot(_.getName.equals("pii"))
          .map(f => col(f.getName())): _*)
      .write
      .option("sep", "\t")
      .option("escape", "")
      .option("quote", "")
      .option("emptyValue", "")
      .option("nullValue", "")
      .mode(SaveMode.Overwrite)
      .csv(enrichConfig.outFolder)
  }

  /**
   * Read the input files.
   * @param inFormat Collector format in which the data is coming in
   * @return A RDD containing strings or byte arrays
   */
  private def getInputRDD(inFormat: String, path: String): RDD[_] =
    inFormat match {
      case "thrift" =>
        MultiInputFormat.setClassConf(classOf[Array[Byte]], hadoopConfig)
        sc.newAPIHadoopFile[
            LongWritable,
            BinaryWritable[Array[Byte]],
            MultiInputFormat[Array[Byte]]
          ](path)
          .map(_._2.get())
      case _ => sc.textFile(path)
    }
}
