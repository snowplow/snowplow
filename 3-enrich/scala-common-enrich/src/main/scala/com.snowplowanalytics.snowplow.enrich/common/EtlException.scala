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
package com.snowplowanalytics.snowplow.enrich.common

// Scalaz
import scalaz._
import Scalaz._

/**
 * The parent for our ETL-specific exceptions
 *
 * Note that the SnowPlow ETL does **not**
 * use exceptions for control flow - it uses
 * Scalaz Validation and ValidationNel objects.
 * 
 * However two types of exception we do support
 * are:
 * 
 * 1. FatalEtlException - should always cause
 *    the ETL to die
 * 2. UnexpectedEtlException - ETL may die or
 *    continue, depending on the ETL config
 */
sealed class EtlException(msg: String) extends RuntimeException(msg)

/**
 * Holds ways of constructing the
 * exception message from a Scalaz
 * Validation or ValidatioNel.
 *
 * Mixed into the companion objects
 * for the exceptions below.
 */
trait EtlExceptionConstructors[E <: EtlException] {
	
  // Structured type lets us pass in
  // a factory to construct our E
  self: {
    val fac: (String => E)
  } =>

  /**
   * Alternative constructor for
   * the companion object.
   *
   * Converts a Scalaz
   * NonEmptyList[String] into a single
   * String error message.
   *
   * @param errs The list of
   *        error messages
   * @return a new EtlException of
   *         type E
   */ 
  def apply(errs: NonEmptyList[String]): E = 
    apply(errs.list)

  /**
   * Alternative constructor for
   * the companion object.
   *
   * Converts a List[String] into
   * a single String error message.
   *
   * @param errs The list of
   *        error messages
   * @return a new EtlException of
   *         type E
   */ 
  def apply(errs: List[String]): E = 
    fac(formatErrors(errs))

  /**
   * A helper to format the list of
   * error messages.
   *
   * @param errs The list of error
   *        messages
   * @return a nicely formatted
   *         error String
   */
  private def formatErrors(errs: List[String]): String =
    "EtlException Errors:\n  - %s".format(errs.mkString("\n  - "))
}

/**
 * Companion object for
 * FatalEtlException
 *
 * Contains an apply() constructor
 * which takes a Scalaz
 * NonEmptyList[String] - see
 * ValidationConstructors trait
 * for details.
 */
object FatalEtlException extends EtlExceptionConstructors[FatalEtlException] {
  val fac = (msg: String) => FatalEtlException(msg)
}

/**
 * Companion object for
 * FatalEtlError
 */
// TODO: delete when Cascading FailureTrap supports exclusions
object FatalEtlError {

  def apply(errs: NonEmptyList[String]): FatalEtlError = 
    apply(errs.list)

  def apply(errs: List[String]): FatalEtlError = 
    FatalEtlError(formatErrors(errs))

  private def formatErrors(errs: List[String]): String =
    "EtlError Errors:\n  - %s".format(errs.mkString("\n  - "))
}

/**
 * Companion object for
 * UnexpectedEtlException
 *
 * Contains an apply() constructor
 * which takes a Scalaz
 * NonEmptyList[String] - see
 * ValidationConstructors trait
 * for details.
 */
object UnexpectedEtlException extends EtlExceptionConstructors[UnexpectedEtlException] {
  val fac = (msg: String) => UnexpectedEtlException(msg)
}

/**
 * A fatal exception in our ETL.
 *
 * Will only be thrown if the ETL cannot
 * feasibly be run - **do not** try to catch
 * it, or a kitten dies.
 *
 * This should be explicitly excluded from
 * Cascading Failure Traps, as soon as they
 * support this (Cascading 2.2).
 */
case class FatalEtlException(msg: String) extends EtlException(msg)

/**
 * A fatal error in our ETL.
 *
 * We are using this as a workaround:
 * because Cascading cannot yet support
 * excluding a specific Exception subclass
 * (e.g. FatalEtlException) from a Failure
 * Trap, we need to throw an Error instead.
 *
 * For details see:
 * https://groups.google.com/forum/?fromgroups=#!topic/cascading-user/Ld5sg1baOyc
 */
// TODO: delete when Cascading FailureTrap supports exclusions
case class FatalEtlError(msg: String) extends Error(msg)

/**
 * An unexpected exception in our
 * ETL.
 *
 * Will be thrown in the event of
 * an unexpected exception. How to
 * handle it will depend on the
 * setting of the Continue On
 * Unexpected Error? flag passed in
 * to the ETL.
 */
case class UnexpectedEtlException(msg: String) extends EtlException(msg)