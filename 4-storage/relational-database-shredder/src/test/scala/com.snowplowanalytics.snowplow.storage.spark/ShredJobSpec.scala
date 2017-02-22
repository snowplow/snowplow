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
package com.snowplowanalytics
package snowplow.storage.spark

import java.io.{BufferedWriter, File, FileWriter, IOException}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

// Commons
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

// Json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse}

// Specs2
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import iglu.client.Resolver

object ShredJobSpec {
  /** Case class representing the input lines written in a file. */
  case class Lines(l: String*) {
    val lines = l.toList

    /** Write the lines to a file. */
    def writeTo(file: File): Unit = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) {
        writer.write(line)
        writer.newLine()
      }
      writer.close()
    }

    def apply(i: Int): String = lines(i)
  }

  /** Case class representing the directories where the output of the job has been written. */
  case class OutputDirs(output: File, badRows: File) {
    /** Delete recursively the output and bad rows directories. */
    def deleteAll(): Unit = List(badRows, output).foreach(deleteRecursively)
  }

  /**
   * Read a part file at the given path into a List of Strings
   * @param root A root filepath
   * @param relativePath The relative path to the file from the root
   * @return the file contents as well as the file name
   */
  def readPartFile(root: File, relativePath: String): Option[(List[String], String)] = {
    val files = listFilesWithExclusions(new File(root, relativePath), List.empty)
      .filter(s => s.contains("part-") && !s.contains("crc"))
    files.headOption match {
      case Some(f) => Some((Source.fromFile(new File(f)).getLines.toList, f))
      case None => None
    }
  }

  /**
   * Recursively list files in a given path, excluding the supplied paths.
   * @param root A root filepath
   * @param exclusions A list of paths to exclude from the listing
   * @return the list of files contained in the root, minus the exclusions
   */
  def listFilesWithExclusions(root: File, exclusions: List[String]): List[String] =
    FileUtils.listFiles(root, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
      .asScala
      .toList
      .map(_.getCanonicalPath)
      .filter(p => !exclusions.contains(p) && !p.contains("crc") && !p.contains("SUCCESS"))

  /** A Specs2 matcher to check if a directory on disk is empty or not. */
  val beEmptyDir: Matcher[File] =
    ((f: File) =>
      !f.exists || (f.isDirectory && f.list().length == 0),
      "is populated dir"
    )

  /**
   * Delete a file or directory and its contents recursively.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) throw new IOException(s"Failed to list files for dir: $file")
        files
      } else {
        Seq.empty[File]
      }
    }

    try {
      if (file.isDirectory) {
        var savedIOException: IOException = null
        for (child <- listFilesSafely(file)) {
          try {
            deleteRecursively(child)
          } catch {
            // In case of multiple exceptions, only last one will be thrown
            case ioe: IOException => savedIOException = ioe
          }
        }
        if (savedIOException != null) throw savedIOException
      }
    } finally {
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) throw new IOException(s"Failed to delete: ${file.getAbsolutePath}")
      }
    }
  }

  /**
   * Make a temporary file optionally filling it with contents.
   * @param tag an identifier who will become part of the file name
   * @param createParents whether or not to create the parent directories
   * @param containing the optional contents
   * @return the created file
   */
  def mkTmpFile(
    tag: String,
    createParents: Boolean = false,
    containing: Option[Lines] = None
  ): File = {
    val f = File.createTempFile(s"snowplow-shred-job-${tag}-", "")
    if (createParents) f.mkdirs() else f.mkdir()
    containing.map(_.writeTo(f))
    f
  }

  /**
   * Create a file with the specified name with a random number at the end.
   * @param tag an identifier who will become part of the file name
   * @return the created file
   */
  def randomFile(tag: String): File =
    new File(System.getProperty("java.io.tmpdir"),
    s"snowplow-shred-job-${tag}-${Random.nextInt(Int.MaxValue)}")

  /** Remove the timestamp from bad rows so that what remains is deterministic */
  def removeTstamp(badRow: String): String = {
    val badRowJson = parse(badRow)
    val badRowWithoutTimestamp =
      ("line", (badRowJson \ "line")) ~ (("errors", (badRowJson \ "errors")))
    compact(badRowWithoutTimestamp)
  }

  private val igluConfig = {
    val encoder = new Base64(true)
    new String(encoder.encode(
      """|{
          |"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
          |"data": {
            |"cacheSize": 500,
            |"repositories": [
              |{
                |"name": "Iglu Central",
                |"priority": 0,
                |"vendorPrefixes": [ "com.snowplowanalytics" ],
                |"connection": {
                  |"http": {
                    |"uri": "http://iglucentral.com"
                  |}
                |}
              |}
            |]
          |}
        |}""".stripMargin.replaceAll("[\n\r]","").getBytes()
    ))
  }
}

/** Trait to mix in in every spec for the shred job. */
trait ShredJobSpec extends SparkSpec {
  import ShredJobSpec._
  val dirs = OutputDirs(randomFile("output"), randomFile("bad-rows"))

  /**
   * Run the shred job with the specified lines as input.
   * @param lines input lines
   */
  def runShredJob(lines: Lines): Unit = {
    val input = mkTmpFile("input", createParents = true, containing = lines.some)
    val config = Array(
      "--input-folder", input.toString(),
      "--output-folder", dirs.output.toString(),
      "--bad-folder", dirs.badRows.toString(),
      "--iglu-config", igluConfig
    )

    val job = ShredJob(spark, config)
    job.run()
    deleteRecursively(input)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dirs.deleteAll()
  }
}
