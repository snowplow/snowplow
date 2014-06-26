/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
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
import java.net.{URI, URISyntaxException}

// RefererParser Java impl
import com.snowplowanalytics.refererparser.{Parser => JParser}
import com.snowplowanalytics.refererparser.{Medium => JMedium}

// Scala
import scala.collection.JavaConversions._ 

/**
 * Enumeration for supported mediums.
 *
 * Replacement for Java version's Enum.
 */
object Medium extends Enumeration {
  type Medium = Value

  val Unknown  = Value("unknown")
  val Search   = Value("search")
  val Internal = Value("internal")
  val Social   = Value("social")
  val Email    = Value("email")

  /**
   * Converts from our Java Medium Enum
   * to our Scala Enumeration values above.
   */
  def fromJava(medium: JMedium) =
    Medium.withName(medium.toString())
}

/**
 * Immutable case class to hold a referer.
 *
 * Replacement for Java version's POJO.
 */
case class Referer(
  medium: Medium.Medium,
  source: Option[String],
  term:   Option[String]
)

/**
 * Parser object - contains one-time initialization
 * of the YAML database of referers, and parse()
 * methods to generate a Referer object from a
 * referer URL.
 *
 * In Java this had to be instantiated as a class.
 */
object Parser {

  type MaybeReferer = Option[Referer]

  private lazy val jp = new JParser()

  private def getHostSafely(uri: URI): String = {
    if (uri == null) {
      null
    } else {
      uri.getHost();
    }
  }

  /**
   * Parses a `refererUri` UR and a `pageUri`
   * URI to return either Some Referer, or None.
   */
  def parse(refererUri: URI, pageUri: URI): MaybeReferer =
    parse(refererUri, getHostSafely(pageUri), Nil);

  /**
   * Parses a `refererUri` UR and a `pageUri`
   * URI to return either Some Referer, or None.
   */
  def parse(refererUri: URI, pageUri: URI, internalDomains: List[String]): MaybeReferer =
    parse(refererUri, getHostSafely(pageUri), internalDomains);

  /**
   * Parses a `refererUri` String and a `pageUri`
   * URI to return either Some Referer, or None.
   */
  def parse(refererUri: String, pageUri: URI): MaybeReferer =
    parse(refererUri, getHostSafely(pageUri), Nil);

  /**
   * Parses a `refererUri` String and a `pageUri`
   * URI to return either Some Referer, or None.
   */
  def parse(refererUri: String, pageUri: URI, internalDomains: List[String]): MaybeReferer =
    parse(refererUri, getHostSafely(pageUri), internalDomains);

  /**
   * Parses a `refererUri` String and a `pageUri`
   * URI to return either some Referer, or None.
   */
  def parse(refererUri: String, pageHost: String): MaybeReferer = {
    parse(refererUri, pageHost, Nil)
  }

  /**
   * Parses a `refererUri` String and a `pageUri`
   * URI to return either some Referer, or None.
   */
  def parse(refererUri: String, pageHost: String, internalDomains: List[String]): MaybeReferer = {

    if (refererUri == null || refererUri == "") {
      None
    } else {
      try {
        parse(new URI(refererUri), pageHost, internalDomains)
      } catch {
        case use: URISyntaxException => None
      }
    }
  }

  /**
   * Parses a `refererUri` URI to return
   * either Some Referer, or None.
   */
  def parse(refererUri: URI, pageHost: String): MaybeReferer = {
    parse(refererUri, pageHost, Nil)
}


  /**
   * Parses a `refererUri` URI to return
   * either Some Referer, or None.
   */
  def parse(refererUri: URI, pageHost: String, internalDomains: List[String]): MaybeReferer = {
    
    try {
      val jrefr = Option(jp.parse(refererUri, pageHost, internalDomains))
      jrefr.map(jr =>
        Referer(Medium.fromJava(jr.medium), Option(jr.source), Option(jr.term))
      )
    } catch {
      case use: URISyntaxException => None
    }
  }
}
