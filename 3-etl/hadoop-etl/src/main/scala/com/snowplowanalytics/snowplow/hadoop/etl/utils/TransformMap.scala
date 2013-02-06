/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package utils

// Java
import java.lang.reflect.Method

// Scalaz
import scalaz._
import Scalaz._

/**
 * HIGHLY EXPERIMENTAL
 *
 * The problem I'm trying to solve: converting maps to classes in Scala
 * is not very easy to do in a functional way, and it gets even harder
 * if you have a class with >22 fields (so can't use case classes).
 *
 * Check out enrichEvent in EnrichmentManager - it uses mutable state to
 * accumulate Scalaz Validation errors and is rapidly getting very clunky
 * and verbose.
 *
 * For a discussion about this on Stack Overflow, see:
 * http://stackoverflow.com/questions/4290955/instantiating-a-case-class-from-a-list-of-parameters
 *
 * The idea I currently have is using Java Reflection with a big ol'
 * TransformationMap:
 * 
 * ("key in map"  -> Tuple2(transformFunc, "field in class"),
 *  "another key" -> Tuple2(transformFunc, "field in class"),
 *  "a third key" -> Tuple2(transformFunc, "field in class"))
 *
 * And then there would be an iteration through the source map which
 * looks up the key in the TransformationMap and then applies
 * the transformFunc using Java Reflection, storing the answer in the
 * class's specified field.
 *
 * If I can get this working, then the next step is to generate
 * a "results" map which contains the results of each
 * transformFunc. Each transformFunc should either return a
 * ValidationNEL[String, Unit], or should be promoted (via an
 * implicit) to the same.
 *
 * Having this should allow me to do something like:
 * resultsMap.foldLeft(Unit.success, |@|) to roll up any validation
 * errors into one final ValidatioNEL. 
 *
 * If I can get all that working, then the final step is to
 * support transformFuncs which set multiple fields. To avoid the
 * complexity spiralling, it would probably be simplest if any
 * transformFunc that wanted to return multiple values returned
 * a TupleN, and then we use the same TupleN for the target fields.
 * Maybe there will be an implicit to convert "raw" target fields
 * into Tuple1s.
 *
 * Okay, let's start...
 */
object DataTransform {

  // Clarificatory aliases
  type Key = String
  type Value = String
  type Field = String

  // A transformation takes a Key and Value and can return anything (for now)
  type TransformFunc = Function2[Key, Value, _]

  // Our source map
  type SourceMap = Map[Key, Value]

  // Our map for transforming data
  type TransformMap = Map[Key, Tuple2[TransformFunc, _]]

  /**
   * An implicit conversion to take any Object and make it
   * Transformable.
   * @param any Any Object
   * @return the new Transformable class, with manifest attached
   */ 
  implicit def makeTransformable[A <: AnyRef](obj: A)(implicit m : Manifest[A]) = new TransformableClass[A](obj)

  /**
   * A helper method to store our functions with accidentally calling them.
   */
  def !~(f: => TransformFunc): TransformFunc = f
}

class TransformableClass[A](obj: A)(implicit m: Manifest[A]) {

  import DataTransform._

  // Let's try to set all fields using reflection
  def transform(sourceMap: SourceMap, transformMap: TransformMap): Unit = {

    val results = sourceMap.map { case (key, in) =>
      if (transformMap.contains(key)) {
        val (func, field) = transformMap(key)
        val out = func(key, in)

        out match {
          case Success(s) =>
            field match {
              case f: String =>
                val result = s.asInstanceOf[AnyRef]
                setters(f).invoke(obj, result)
                1.successNel[String] // +1 to the count of fields successfully set
              case Tuple2(f1: String, f2: String) =>
                val result = s.asInstanceOf[Tuple2[AnyRef, AnyRef]]
                setters(f1).invoke(obj, result._1)
                setters(f2).invoke(obj, result._2)
                2.successNel[String] // +2 to the count of fields successfully set
            }
          case Failure(e) =>
            e.failNel[Int]
        }
      } else {
        0.successNel[String] // Key not found: zero fields updated
      }
    }.toList

    // TODO: can we now roll up all the results?
  }

  // Do all the reflection for the setters we need:
  // This needs to be lazy because Method is not serializable
  lazy val setters = getSetters

  def lowerFirst(s : String) = s.substring(0,1).toLowerCase + s.substring(1)
  // Cut out "set" and lower case the first after
  def setterToFieldName(setter : Method) = lowerFirst(setter.getName.substring(3))

  def getSetters = m.erasure
    .getDeclaredMethods
    .filter { _.getName.startsWith("set") }
    .groupBy { setterToFieldName(_) }
    .mapValues { _.head }
}