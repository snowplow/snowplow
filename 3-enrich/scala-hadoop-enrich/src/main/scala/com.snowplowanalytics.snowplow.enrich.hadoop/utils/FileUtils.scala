/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package utils

// Java
import java.io.File
import java.net.URI

// Scalaz
import scalaz._
import Scalaz._

// Apache
import org.apache.commons.io.{FileUtils => FU}

// Hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache

/**
 * General-purpose utils to help with file
 * processing, including on HDFS.
 */
object FileUtils {

  /**
   * A helper to source a hosted asset. If the asset's path
   * starts with http(s)://, then it's a web-hosted asset: we
   * need to download it and add it to HDFS.
   * Otherwise, we expect the asset is available from S3 or
   * HDFS - no action needed.
   *
   * @param conf Our Hadoop job Configuration
   * @param assetUri The hosted asset's URI
   * @return the URI to the local asset, readable from HDFS
   */
  def sourceFile(conf: Configuration, fileUri: URI): Validation[String, URI] = {
    fileUri.getScheme match {
      case "http"   | "https"    => FileUtils.downloadToHdfs(conf, fileUri).toUri.success
      case "s3" | "s3n" | "hdfs" => fileUri.success
      case s => "Scheme [%s] for file [%s] not supported".format(s, fileUri).fail
    }
  }

  /**
   * A helper to download an asset file and add it to HDFS.
   * We need this because unfortunately EMR cannot read an
   * S3 file from a third-party account, even if public.
   *
   * @param conf Our Hadoop job Configuration
   * @param assetUri The URI of the asset to download
   * @return the path to the asset in HDFS
   */
  def downloadToHdfs(conf: Configuration, assetUri: URI): Path = {

    val filename = extractFilenameFromUri(assetUri)
    val localFile = "/tmp/%s".format(filename)
    FU.copyURLToFile(assetUri.toURL, new File(localFile), 60000, 300000) // Sensible conn, read timeouts
    
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path("hdfs:///cache/%s".format(filename))
    fs.copyFromLocalFile(true, true, new Path(localFile), hdfsPath) // Delete local file, overwrite target if exists
    hdfsPath
  }

  /**
   * Adds a file to the DistributedCache as a symbolic link.
   * Works with S3 and HDFS URIs.
   *
   * @param conf Our current job Configuration
   * @param source The file to add to the DistributedCache
   * @param symlink Name of our symbolic link in the cache
   * @return the path to the symbolic link in the cache
   */
  def addToDistCache(conf: Configuration, source: URI, symlink: String): String = {
    val path = source.toString + "#" + symlink
    DistributedCache.createSymlink(conf)
    DistributedCache.addCacheFile(new URI(path), conf)
    "./" + symlink
  }

  /**
   * A helper to get the filename from a URI.
   *
   * @param uri The URL to extract the filename from
   * @return the extracted filename
   */
  private def extractFilenameFromUri(uri: URI): String = {
    val p = uri.getPath
    p.substring(p.lastIndexOf('/') + 1, p.length)
  }
}