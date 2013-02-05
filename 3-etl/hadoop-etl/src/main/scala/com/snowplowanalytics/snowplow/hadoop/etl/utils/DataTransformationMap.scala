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

  // Clarificatory types
  type Key = String
  type Value = String
  type Field = String

  // A transformation takes a string and can return anything
  type TransformFunc1 = Value => AnyRef

  // Our source map
  type SourceMap1 = Map[Key, Value]

  // Our map for transforming data
  type TransformMap1 = Map[Key, Tuple2[TransformFunc1, Field]]
}