/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package jobs

// Java
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

// Apache Commons IO
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Scala
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.collection.JavaConverters._

// Scalaz
import scalaz._
import Scalaz._

// Scala
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Scalding
import com.twitter.scalding._

// Specs2
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._
/**
 * Holds helpers for running integration
 * tests on SnowPlow EtlJobs.
 */
object JobSpecHelpers {

  /**
   * A Specs2 matcher to check if a Scalding
   * output sink is empty or not.
   */
  val beEmpty: Matcher[ListBuffer[_]] =
    ((_: ListBuffer[_]).isEmpty, "is not empty")

  /**
   * A Specs2 matcher to check if a directory
   * on disk is empty or not.
   */
  val beEmptyFile: Matcher[File] =
    ((f: File) =>
      !f.exists || (f.isFile && f.length == 0),
      "is populated file"
    )

  /**
   * How Scalding represents input lines
   */
  type ScaldingLines = List[(String, String)]

  /**
   * Base64-urlsafe encoded version of this standard
   * Iglu configuration.
   */
  private val IgluConfig = {
    val encoder = new Base64(true) // true means "url safe"
    new String(encoder.encode(SpecHelpers.IgluConfig.getBytes)
    )
  }

  /**
    * Base64-urlsafe encoded version of local (test-only) DynamoDB duplicates storage
    */
  private val DuplicateStorageConfig = {
    val encoder = new Base64(true) // true means "url safe"
    new String(encoder.encode(SpecHelpers.DuplicateStorageConfig.getBytes)
    )
  }

  /**
   * A case class to make it easy to write out input
   * lines for Scalding jobs without manually appending
   * line numbers.
   *
   * @param l The repeated String parameters
   */
  case class Lines(l: String*) {

    val lines = l.toList
    val numberedLines = number(lines)

    /**
     * Writes the lines to the given file
     *
     * @param file The file to write the
     *        lines to
     */
    def writeTo(file: File) = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) {
        writer.write(line)
        writer.newLine()
      }
      writer.close()
    }

    /**
     * Numbers the lines in the Scalding format.
     * Converts "My line" to ("0" -> "My line")
     *
     * @param lines The List of lines to number
     * @return the List of ("line number" -> "line")
     *         tuples.
     */
    private def number(lines: List[String]): ScaldingLines =
      for ((l, n) <- lines zip (0 until lines.size)) yield (n.toString -> l)
  }

  /**
   * Implicit conversion from a Lines object to
   * a ScaldingLines, aka List[(String, String)],
   * ready for Scalding to use.
   *
   * @param lines The Lines object
   * @return the ScaldingLines ready for Scalding
   */
  implicit def Lines2ScaldingLines(lines : Lines): ScaldingLines = lines.numberedLines 

  /**
   * A standard JobSpec definition used by all of our
   * integration tests.
   */
  val ShredJobSpec = 
    JobTest("com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob").
      arg("input_folder", "inputFolder").
      arg("output_folder", "outputFolder").
      arg("bad_rows_folder", "badFolder").
      arg("exceptions_folder", "exceptionsFolder").
      arg("iglu_config", IgluConfig)

  case class Sinks(output: File, badRows: File, exceptions: File) {
    def deleteAll() {
      for (f <- List(exceptions, badRows, output)) {
        f.delete()
      }
    }
  }

  /**
   * Run the ShredJob using the Scalding Tool.
   *
   * @param lines The input lines to shred
   * @return a Tuple3 containing open File
   *         objects for the output, bad rows
   *         and exceptions temporary directories.
   */
  def runJobInTool(lines: Lines): Sinks = {

    def mkTmpDir(tag: String, createParents: Boolean = false, containing: Option[Lines] = None): File = {
      val f = File.createTempFile(s"snowplow-shred-job-${tag}-", "")
      if (createParents) f.mkdirs() else f.mkdir()
      containing.map(_.writeTo(f))
      f
    }

    val input      = mkTmpDir("input", createParents = true, containing = lines.some)
    val output     = mkTmpDir("output")
    val badRows    = mkTmpDir("bad-rows")
    val exceptions = mkTmpDir("exceptions")  

    val duplicateStorageArgs = if (SpecHelpers.CI) List("--duplicate_storage_config", DuplicateStorageConfig) else Nil

    val args: Array[String] = (List[String]("com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob", "--local",
      "--input_folder",             input.getAbsolutePath,
      "--output_folder",            output.getAbsolutePath,
      "--bad_rows_folder",          badRows.getAbsolutePath,
      "--exceptions_folder",        exceptions.getAbsolutePath,
      "--iglu_config",              IgluConfig) ++
      duplicateStorageArgs
      ).toArray

    // Execute
    Tool.main(args)
    input.delete()

    Sinks(output, badRows, exceptions)
  }

  /**
   * Removes the timestamp from bad rows so that what remains is deterministic
   *
   * @param badRow
   * @return The bad row without the timestamp
   */
  def removeTstamp(badRow: String): String = {
    val badRowJson = parse(badRow)
    val badRowWithoutTimestamp = ("line", (badRowJson \ "line")) ~ ("errors", (badRowJson \ "errors"))
    compact(badRowWithoutTimestamp)
  }

  /**
   * Reads a file at the given path into a List of Strings
   *
   * @param root A root filepath
   * @param relativePath The relative path to the file from
   *        the root
   * @return the file contents
   */
  def readFile(root: File, relativePath: String): List[String] =
    Source
      .fromFile(new File(root, relativePath))
      .getLines
      .toList

  /**
   * Recursively lists files in a given path, excluding the
   * supplied paths.
   *
   * @param root A root filepath
   * @param exclusions A list of paths to exclude from the listing
   * @return the list of files contained in the root, minus the
   *         exclusions
   */
  def listFilesWithExclusions(root: File, exclusions: List[String]): List[String] = {
    val excl = for {
      e <- exclusions
    } yield (new File(root, e)).getCanonicalPath

    FileUtils.listFiles(root, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
      .asScala
      .toList
      .map(_.getCanonicalPath)
      .filter(p => !excl.contains(p))
  }

}
