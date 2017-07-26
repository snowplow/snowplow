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

import scala.language.implicitConversions

import cats.data.{Validated, ValidatedNel, NonEmptyList => NonEmptyListCats}

import scalaz.{Failure, Success, Validation, ValidationNel, NonEmptyList => NonEmptyListZ}

import org.json4s.JsonAST._

import io.circe.Json

import com.snowplowanalytics.iglu.client.Resolver

import com.github.fge.jsonschema.core.report.ProcessingMessage

// This project
import Common._
import LoaderError._


/**
 * Module containing conversions between various parts
 * of Snowplow apps and libs
 */
private[rdbloader] object Compat {

  implicit def validated[L, R](z: Validation[L, R]): Validated[L, R] = z match {
    case Success(a) => Validated.Valid(a)
    case Failure(list) => Validated.Invalid(list)
  }

  implicit def nel[A](z: NonEmptyListZ[A]): NonEmptyListCats[A] =
    NonEmptyListCats(z.head, z.tail)

  implicit def validMes(v: ProcessingMessage): ConfigError =
    ValidationError(v.getMessage)

  implicit def validatedNel[L, R](z: ValidationNel[L, R]): ValidatedNel[L, R] =
    z.fold((l: NonEmptyListZ[L]) => Validated.Invalid(l), r => Validated.Valid(r))

  def convertIgluResolver(json: JValue): ValidatedNel[ConfigError, Resolver] = {
    val resolver: ValidatedNel[ProcessingMessage, Resolver] = Resolver.parse(json)
    resolver.leftMapNel(x => ValidationError(x.getMessage))
  }

  implicit def jvalueToCirce(jValue: JValue): Json = jValue match {
    case JString(s)    => Json.fromString(s)
    case JObject(vals) => Json.fromFields(vals.map { case (k, v) => (k, jvalueToCirce(v))})
    case JInt(i)       => Json.fromBigInt(i)
    case JDouble(d)    => Json.fromDoubleOrNull(d)
    case JBool(b)      => Json.fromBoolean(b)
    case JArray(arr)   => Json.arr(arr.map(jvalueToCirce): _*)
    case JNull         => Json.Null
    case JDecimal(d)   => Json.fromBigDecimal(d)
    case JNothing      => Json.Null
  }
}
