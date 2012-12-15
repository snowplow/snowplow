/**
 * Copyright 2012 SnowPlow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowplowanalytics.refererparser.scala

// Java
import java.net.URI

// RefererParser Java impl
import com.snowplowanalytics.refererparser.{Parser => JParser}

/**
 * Parser object - contains one-time initialization
 * of the YAML database of referers, and parse()
 * methods to generate a Referer object from a
 * referer URL.
 *
 * In Java this had to be instantiated as a class.
 */
object Parser {

  private type MaybeReferer = Option[Referer]

  /**
   * Parses a `refererUri` String to return
   * either a Referer, or None.
   */
  def parse(refererUri: String): MaybeReferer = {
    val uri = new URI(refererUri)
    parse(uri)
  }

  /**
   * Parses a `refererUri` URI to return
   * either a Referer, or None.
   */
  def parse(refererUri: URI): MaybeReferer = {
    val jp = new JParser()
    val r = jp.parse(refererUri)
    
    if (r.known)
      Referer(
        name = r.name,
        searchParameter = r.searchParameter,
        searchTerm = r.searchTerm,
        uri = r.uri
      )
    else
      None
  }
}