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
package com.snowplowanalytics.snowplow

import cats.Functor
import cats.data._
import cats.free.Free
import cats.implicits._

import rdbloader.config.Step
import rdbloader.LoaderError.{DiscoveryError, DiscoveryFailure}

package object rdbloader {
  /**
   * Main RDB Loader type. Represents all IO happening
   * during discovering, loading and monitoring.
   * End of the world type, that must be unwrapped and executed
   * using one of interpreters
   */
  type Action[A] = Free[LoaderA, A]

  /**
   * Loading effect, producing value of type `A`,
   * that also mutates state, until short-circuited
   * on failure `F`
   *
   * @tparam F failure, short-circuiting whole computation
   * @tparam A value of computation
   */
  type TargetLoading[F, A] = EitherT[StateT[Action, List[Step], ?], F, A]

  /**
   * IO-free result validation
   */
  type DiscoveryStep[A] = Either[DiscoveryFailure, A]

  /**
   * IO Action that can aggregate failures
   */
  type ActionValidated[A] = Action[ValidatedNel[DiscoveryFailure, A]]

  /**
   * FullDiscovery discovery process,
   */
  type Discovery[A] = Action[Either[DiscoveryError, A]]

  val Discovery = Functor[Action].compose[Either[DiscoveryError, ?]]

  /**
   * Single discovery step
   */
  type DiscoveryAction[A] = Action[DiscoveryStep[A]]

  /**
   * Composed functor of IO and discovery step
   */
  private[rdbloader] val DiscoveryAction =
    Functor[Action].compose[DiscoveryStep]

  /**
   * Lift stateless `Action[Either[A, B]]` computation into
   * computation that has state with last successful
   * state in `current`, but also can short-circuit
   * with error message
   */
  implicit class StatefulLoading[A, B](val loading: Action[Either[B, A]]) extends AnyVal {
    def addStep(current: Step): TargetLoading[B, A] = {
      EitherT(StateT((last: List[Step]) => loading.map {
        case Right(e) => (current :: last, Right(e))
        case Left(e) => (last, Left(e))
      }))
    }

    def withoutStep: TargetLoading[B, A] =
      EitherT(StateT((last: List[Step]) => loading.map(e => (last, e))))
  }

  implicit class AggregateErrors[A, B](eithers: List[Either[A, B]]) {
    def aggregatedErrors: ValidatedNel[A, List[B]] =
      eithers.map(_.toValidatedNel).sequence
  }
}
