/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow.enrich
package stream

import java.io.File

object config {

  final case class FileConfig(
    config: File = new File("."),
    resolver: String = "",
    enrichmentsDir: Option[String] = None,
    forceDownload: Boolean = false
  )

  trait FileConfigOptions { self: scopt.OptionParser[FileConfig] =>

    val FilepathRegex = "^file:(.+)$".r
    private val regexMsg = "'file:[filename]'"

    def configOption(): Unit =
      opt[File]("config")
        .required()
        .valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(config = f))
        .validate(
          f =>
            if (f.exists) success
            else failure(s"Configuration file $f does not exist")
        )
    def localResolverOption(): Unit =
      opt[String]("resolver")
        .required()
        .valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
    def localEnrichmentsOption(): Unit =
      opt[String]("enrichments")
        .optional()
        .valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
    def forceCachedFilesDownloadOption(): Unit =
      opt[Unit]("force-cached-files-download")
        .text("Invalidate the cached IP lookup / IAB database files and download them anew")
        .action((_, c) => c.copy(forceDownload = true))
  }
}
