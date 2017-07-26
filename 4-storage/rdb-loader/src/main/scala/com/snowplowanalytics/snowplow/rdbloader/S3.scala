/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader

import com.amazonaws.services.s3.model.S3ObjectSummary

import cats.syntax.either._

import shapeless.tag
import shapeless.tag._

import io.circe.Decoder

/**
 * Common types and functions for Snowplow S3 clients
 */
object S3 {

  /**
   * Refined type for AWS S3 bucket, allowing only valid S3 paths
   * (with `s3://` prefix and trailing shash)
   */
  type Folder = String @@ S3FolderTag

  object Folder extends tag.Tagger[S3FolderTag] {

    def parse(s: String): Either[String, Folder] = s match {
      case _ if !correctlyPrefixed(s) => "Bucket name must start with s3:// prefix".asLeft
      case _ if s.length > 1024        => "Key length cannot be more than 1024 symbols".asLeft
      case _                           => coerce(s).asRight
    }

    def coerce(s: String): Folder =
      apply(appendTrailingSlash(fixPrefix(s)).asInstanceOf[Folder])

    def append(s3Bucket: Folder, s: String): Folder = {
      val normalized = if (s.endsWith("/")) s else s + "/"
      coerce(s3Bucket + normalized)
    }

    def getParent(key: Folder): Folder = {
      val string = key.split("/").dropRight(1).mkString("/")
      coerce(string)
    }

    private def correctlyPrefixed(s: String): Boolean =
      supportedPrefixes.foldLeft(false) { (result, prefix) =>
        result || s.startsWith(s"$prefix://")
      }

    private def appendTrailingSlash(s: String): String =
      if (s.endsWith("/")) s
      else s + "/"

  }

  /**
   * Extract `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events` part from
   * Set of prefixes that can be used in config.yml
   * In the end it won't affect how S3 is accessed
   */
  val supportedPrefixes = Set("s3", "s3n", "s3a")

  private def correctlyPrefixed(s: String): Boolean =
    supportedPrefixes.foldLeft(false) { (result, prefix) =>
      result || s.startsWith(s"$prefix://")
    }


  /**
   * Extract `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events/` part from
   * `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events/somefile`
   *
   * @param s string probably containing run id and atomic events subpath
   * @return string refined as folder
   */
  def getAtomicPath(s: Key): Option[Folder] =
    s match {
      case loaders.Common.atomicSubpathPattern(prefix, subpath, _) =>
        Some(Folder.coerce(prefix + "/" + subpath))
      case _ => None
    }

  /**
   * Refined type for AWS S3 key,  allowing only valid S3 paths
   * (with `s3://` prefix and without trailing shash)
   */
  type Key = String @@ S3KeyTag

  object Key extends tag.Tagger[S3KeyTag] {

    def getParent(key: Key): Folder = {
      val string = key.split("/").dropRight(1).mkString("/")
      Folder.coerce(string)
    }

    def coerce(s: String): Key =
      fixPrefix(s).asInstanceOf[Key]

    def parse(s: String): Either[String, Key] = s match {
      case _ if !correctlyPrefixed(s) => "S3 key must start with s3:// prefix".asLeft
      case _ if s.length > 1024       => "Key length cannot be more than 1024 symbols".asLeft
      case _ if s.endsWith("/")       => "S3 key cannot have trailing slash".asLeft
      case _                          => coerce(s).asRight
    }
  }

  /**
   * Transform S3 object summary into valid S3 key string
   */
  def getKey(s3ObjectSummary: S3ObjectSummary): S3.Key =
    S3.Key.coerce(s"s3://${s3ObjectSummary.getBucketName}/${s3ObjectSummary.getKey}")

  // Tags for refined types
  sealed trait S3FolderTag
  sealed trait S3KeyTag
  sealed trait AtomicEventsKeyTag

  implicit val s3FolderDecoder: Decoder[Folder] =
    Decoder.decodeString.emap(Folder.parse)

  /**
   * Split S3 path into bucket name and folder path
   *
   * @param path S3 full path with `s3://` and with trailing slash
   * @return pair of bucket name and remaining path ("some-bucket", "some/prefix/")
   */
  private[rdbloader] def splitS3Path(path: Folder): (String, String) =
    path.stripPrefix("s3://").split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
      case Nil => throw new IllegalArgumentException(s"Invalid S3 bucket path was passed")  // Impossible
    }

  /**
   * Split S3 key into bucket name and filePath
   *
   * @param key S3 full path with `s3://` prefix and without trailing slash
   * @return pair of bucket name and remaining path ("some-bucket", "some/prefix/")
   */
  private[rdbloader] def splitS3Key(key: Key): (String, String) =
    key.stripPrefix("s3://").split("/").toList match {
      case head :: tail => (head, tail.mkString("/").stripSuffix("/"))
      case _ => throw new IllegalArgumentException(s"Invalid S3 key [$key] was passed")  // Impossible
    }

  private def fixPrefix(s: String): String =
    if (s.startsWith("s3n")) "s3" + s.stripPrefix("s3n")
    else if (s.startsWith("s3a")) "s3" + s.stripPrefix("s3a")
    else s
}
