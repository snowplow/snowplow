/* 
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
        referer = r.referer,
        searchParameter = r.searchParameter,
        searchTerm = r.searchTerm,
        uri = r.uri
      )
    else
      None
  }
}