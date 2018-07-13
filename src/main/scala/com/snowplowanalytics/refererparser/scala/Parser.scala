/**
 * Copyright 2012-2018 Snowplow Analytics Ltd
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
import java.io.InputStream

// RefererParser Java impl
import com.snowplowanalytics.refererparser.{Parser => JParser}
import com.snowplowanalytics.refererparser.{Medium => JMedium}

// Scala
import scala.collection.JavaConversions._

// Cats Effect
import cats.effect.Sync
import cats.syntax.either._
import cats.syntax.flatMap._

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
  val Paid     = Value("paid")

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
  term: Option[String]
)

/**
 * Parser object - contains one-time initialization
 * of the JSON database of referers, and parse()
 * methods to generate a Referer object from a
 * referer URL.
 */
object Parser {
  private lazy val parser = new Parser()

  def parse[F[_]: Sync](
    refererUri: String,
  ): F[Option[Referer]] =
    parser.parse(refererUri)

  def parse[F[_]: Sync](
    refererUri: URI
  ): F[Option[Referer]] =
    parser.parse(refererUri)

  def parse[F[_]: Sync](
    refererUri: String,
    pageHost: String
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageHost)

  def parse[F[_]: Sync](
    refererUri: URI,
    pageHost: String
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageHost)

  def parse[F[_]: Sync](
    refererUri: String,
    pageUri: URI
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageUri)

  def parse[F[_]: Sync](
    refererUri: URI,
    pageUri: URI
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageUri)

  def parse[F[_]: Sync](
    refererUri: String,
    pageHost: Option[String],
    internalDomains: List[String]
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageHost, internalDomains)

  def parse[F[_]: Sync](
    refererUri: URI,
    pageHost: Option[String],
    internalDomains: List[String]
  ): F[Option[Referer]] =
    parser.parse(refererUri, pageHost, internalDomains)
}

/**
 * Parser class - Scala version of Java Parser, with
 * everything wrapped in Sync
 */
class Parser(maybeReferersStream: Option[InputStream] = None) {

  private val jp = maybeReferersStream match {
    case Some(stream) => new JParser(stream)
    case None => new JParser()
  }

  private def toUri(uri: String): Option[URI] = {
    if (uri == "")
      None
    else
      Either.catchNonFatal(new URI(uri)).toOption
  }

  def this(referersStream: InputStream) = this(Some(referersStream))

  def parse[F[_]: Sync](
    refererUri: String
  ): F[Option[Referer]] =
    toUri(refererUri)
      .map(uri => parse(uri, None, Nil))
      .getOrElse(Sync[F].pure(None))

  def parse[F[_]: Sync](
    refererUri: URI
  ): F[Option[Referer]] =
    parse(refererUri, None, Nil)

  def parse[F[_]: Sync](
    refererUri: String,
    pageHost: String
  ): F[Option[Referer]] =
    toUri(refererUri)
      .map(uri => parse(uri, Some(pageHost), Nil))
      .getOrElse(Sync[F].pure(None))

  def parse[F[_]: Sync](
    refererUri: String,
    pageUri: URI
  ): F[Option[Referer]] =
    toUri(refererUri)
      .map(uri => parse(uri, Some(pageUri.getHost), Nil))
      .getOrElse(Sync[F].pure(None))

  def parse[F[_]: Sync](
    refererUri: URI,
    pageHost: String
  ): F[Option[Referer]] =
    parse(refererUri, Some(pageHost), Nil)

  def parse[F[_]: Sync](
    refererUri: URI,
    pageUri: URI
  ): F[Option[Referer]] =
    parse(refererUri, Some(pageUri.getHost), Nil)

  def parse[F[_]: Sync](
    refererUri: String,
    pageHost: Option[String],
    internalDomains: List[String]
  ): F[Option[Referer]] =
    toUri(refererUri)
      .map(uri => parse(uri, pageHost, internalDomains))
      .getOrElse(Sync[F].pure(None))

  /**
   * Parses a `refererUri` URI to return
   * either Some Referer, or None.
   */
  def parse[F[_]: Sync](
    refererUri: URI,
    pageHost: Option[String],
    internalDomains: List[String]
  ): F[Option[Referer]] = {
    Sync[F].delay {
      try {
        Option(jp.parse(refererUri, pageHost.getOrElse(null), internalDomains))
          .map(
            jr =>
              Referer(
                Medium.fromJava(jr.medium),
                Option(jr.source),
                Option(jr.term)
            ))
      } catch {
        case use: URISyntaxException => None
      }
    }
  }
}
