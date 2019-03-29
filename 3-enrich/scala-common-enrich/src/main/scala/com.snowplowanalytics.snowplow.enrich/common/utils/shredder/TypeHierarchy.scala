/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.utils.shredder

/** Expresses the hierarchy of types for this type. */
final case class TypeHierarchy(
  val rootId: String,
  val rootTstamp: String,
  val refRoot: String,
  val refTree: List[String],
  val refParent: String
) {

  /**
   * Completes a partial TypeHierarchy with the supplied refTree elements, and uses
   * the final refTree to replace the refParent too.
   * @param refTree the rest of the type tree to append onto existing refTree
   * @return the completed TypeHierarchy
   */
  def complete(refTree: List[String]): TypeHierarchy = {
    val full = this.refTree ++ refTree
    this.copy(refTree = full, refParent = secondTail(full))
  }

  /**
   * Get the last-but-one element ("tail-tail") from a list.
   * @param ls The list to return the last-but-one element from
   * @return the last-but-one element from this list
   */
  private def secondTail[A](ls: List[A]): A = ls match {
    case h :: _ :: Nil => h
    case _ :: tail => secondTail(tail)
    case _ => throw new NoSuchElementException
  }

}
