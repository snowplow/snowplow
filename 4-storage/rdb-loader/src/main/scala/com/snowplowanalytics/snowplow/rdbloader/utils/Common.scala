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
package utils

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.util.control.NonFatal

import cats.data._
import cats.implicits._

import org.json4s.JValue
import org.json4s.jackson.{parseJson => parseJson4s}

import io.circe._

// This project
import LoaderError._
import config.Step

/**
 * Various common utility functions
 */
object Common {

  /**
   * Remove all occurrences of access key id and secret access key from message
   * Helps to avoid publishing credentials on insecure channels
   *
   * @param message original message that may contain credentials
   * @param stopWords list of secret words (such as passwords) that should be sanitized
   * @return string with hidden keys
   */
  def sanitize(message: String, stopWords: List[String]): String =
    stopWords.foldLeft(message) { (result, secret) =>
      result.replaceAll(secret, "x" * secret.length)
    }

  /**
   * Generate result for end-of-the-world log message using loading result
   *
   * @param result loading process state
   * @return log entry, which can be interpreted accordingly
   */
  def interpret(result: (List[Step], Either[LoaderError, Unit])): Log = {
    result match {
      case (steps, Right(_)) => Log.LoadingSucceeded(steps.reverse)
      case (steps, Left(error)) => Log.LoadingFailed(error.show, steps.reverse)
    }
  }

  /**
   * Transforms CamelCase string into snake_case
   * Also replaces all hyphens with underscores
   *
   * @see https://github.com/snowplow/iglu/blob/master/0-common/schema-ddl/src/main/scala/com.snowplowanalytics/iglu.schemaddl/StringUtils.scala
   * @param str string to transform
   * @return the underscored string
   */
  def toSnakeCase(str: String): String =
    str.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("-", "_")
      .replaceAll("""\.""", "_")
      .toLowerCase


  private val m = ru.runtimeMirror(getClass.getClassLoader)

  /**
   * Common trait for all ADTs that can be read from string
   * Must be extended by sealed hierarchy including only singletons
   * Used by `decodeStringEnum` to get runtime representation of whole ADT
   */
  trait StringEnum {
    /**
      * **IN** string representation.
      * It should be used only to help read `StringEnum` from string
      * and never other way round, such as render value into SQL statement
      */
    def asString: String
  }

  /**
   * Derive decoder for ADT with `StringEnum`
   *
   * @tparam A sealed hierarchy
   * @return circe decoder for ADT `A`
   */
  def decodeStringEnum[A <: StringEnum: TypeTag]: Decoder[A] =
    Decoder.instance(parseEnum[A])

  /**
    * Helper method to parse Json4s JSON AST without exceptions
    * Truncate invalid JSON in error to prevent exposing credentials
    *
    * @param json string presumably containing valid JSON
    * @return json4s JValue AST if success
    */
  def safeParse(json: String): Either[ConfigError, JValue] =
    try {
      parseJson4s(json).asRight
    } catch {
      case NonFatal(_) => ParseError(s"Invalid JSON [${json.take(10)}...]").asLeft
    }

  /**
   * Parse element of `StringEnum` sealed hierarchy from circe AST
   *
   * @param hCursor parser's cursor
   * @tparam A sealed hierarchy
   * @return either successful circe AST or decoding failure
   */
  private def parseEnum[A <: StringEnum: TypeTag](hCursor: HCursor): Decoder.Result[A] = {
    for {
      string <- hCursor.as[String]
      method  = fromString[A](string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  /**
   * Parse element of `StringEnum` sealed hierarchy from String
   *
   * @param string line containing `asString` representation of `StringEnum`
   * @tparam A sealed hierarchy
   * @return either successful circe AST or decoding failure
   */
  def fromString[A <: StringEnum: TypeTag](string: String): Either[String, A] = {
    val map = sealedDescendants[A].map { o => (o.asString, o) }.toMap
    map.get(string) match {
      case Some(a) => Right(a)
      case None => Left(s"Unknown ${typeOf[A].typeSymbol.name.toString} [$string]")
    }
  }

  /**
   * Get all objects extending some sealed hierarchy
   * @tparam Root some sealed trait with object descendants
   * @return whole set of objects
   */
  def sealedDescendants[Root: TypeTag]: Set[Root] = {
    val symbol = typeOf[Root].typeSymbol
    val internal = symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
    val descendants = if (internal.isSealed)
      Some(internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - symbol)
    else None
    descendants.getOrElse(Set.empty).map(x => getCaseObject(x).asInstanceOf[Root])
  }

  /**
   * Reflection method to get runtime object by compiler's `Symbol`
   * @param desc compiler runtime `Symbol`
   * @return "real" scala case object
   */
  private def getCaseObject(desc: Symbol): Any = {
    val mod = m.staticModule(desc.asClass.fullName)
    m.reflectModule(mod).instance
  }

  /**
   * Syntax extension to transform `Either` with string as failure
   * into circe-appropriate decoder result
   */
  implicit class ParseErrorOps[A](val error: Either[String, A]) extends AnyVal {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  /**
   * Syntax extension to parse JSON objects with known keys
   */
  implicit class JsonHashOps(val obj: Map[String, Json]) extends AnyVal {
    def getKey(key: String, hCursor: HCursor): Decoder.Result[Json] = obj.get(key) match {
      case Some(success) => Right(success)
      case None => Left(DecodingFailure(s"Key [$key] is missing", hCursor.history))
    }
  }

  /**
   * Syntax extension to transform left type in `ValidationNel`
   */
  implicit class LeftMapNel[L, R](val validation: ValidatedNel[L, R]) extends AnyVal {
    def leftMapNel[LL](l: L => LL): ValidatedNel[LL, R] =
      validation.leftMap(_.map(l))
  }

  /**
   * Extract integer from string if it contains only valid number
   */
  object IntString {
    def unapply(s: String): Option[Int] =
      try { Some(s.toInt) } catch { case _: NumberFormatException => None }
  }
}
