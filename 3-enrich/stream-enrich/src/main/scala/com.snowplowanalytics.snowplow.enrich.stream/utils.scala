/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich.stream

import scala.util.{Failure, Success, Try}

import scalaz._
import Scalaz._

object utils {
  // to rm once 2.12 as well as the right projections
  def fold[A, B](t: Try[A])(ft: Throwable => B, fa: A => B): B = t match {
    case Success(a) => fa(a)
    case Failure(t) => ft(t)
  }

  def toValidation[A](t: Try[A]): \/[Throwable, A] = fold(t)(_.left, _.right)

  def filterOrElse[L, R](e: Either[L, R])(p: R => Boolean, l: => L): Either[L, R] = e match {
    case Right(r) if p(r) => Right(r)
    case Right(_)         => Left(l)
    case o                => o
  }
}