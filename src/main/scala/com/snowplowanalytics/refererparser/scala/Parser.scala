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
 * Immutable case class to hold a referal.
 *
 * Replacement for Java version's POJO.
 */
case class Referal(referer: Referer, search: Option[Search])

/**
 * Immutable case class to hold a referer.
 *
 * Replacement for Java version's POJO.
 */
case class Referer(name: String)

/**
 * Immutable case class to hold a search.
 *
 * Replacement for Java version's POJO.
 */
case class Search(term: String, parameter: String)

/**
 * Parser object - contains one-time initialization
 * of the YAML database of referers, and parse()
 * methods to generate a Referer object from a
 * referer URL.
 *
 * In Java this had to be instantiated as a class.
 */
object Parser {

  private type MaybeReferal = Option[Referal]

  /**
   * Parses a `refererUri` String to return
   * either a Referal, or None.
   */
  def parse(refererUri: String): MaybeReferal =
    if (refererUri == null || refererUri == "")
      None
    else
      parse(new URI(refererUri))

  /**
   * Parses a `refererUri` URI to return
   * either a Referal, or None.
   */
  def parse(refererUri: URI): MaybeReferal = {
    val jp = new JParser()
    
    for {
      r <- Option(jp.parse(refererUri))
      s <- Option(r.search)
    } yield Referal(Referer(name = r.referer.name),
                    Option(Search(term = s.term,
                                  parameter = s.parameter)))
  }
}