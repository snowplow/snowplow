/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package outputs

// Cascading
import cascading.tuple.Fields
import cascading.tuple.TupleEntry
import cascading.tap.partition.Partition

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey
/**
 * Custom Partition to write out our JSONs into
 * schema-delimited paths.
 */
class ShreddedPartition(val partitionFields: Fields) extends Partition {

  def getPartitionFields(): Fields = partitionFields
  def getPathDepth(): Int = 4 // vendor/name/format/version
  
  def toPartition(tupleEntry: TupleEntry): String = {

    val fields = tupleEntry.asIterableOf(classOf[String]).toList
    val schemaUri = fields match {
      case key :: Nil => key
      case _ =>
        throw new IllegalArgumentException(s"ShreddedPartition expects 1 field; got ${fields.size}")
    }

    // Round-tripping through a SchemaKey ensures we have a valid path
    SchemaKey.parse(schemaUri) match {
      case Failure(err) =>
        throw new IllegalArgumentException("ShreddedPartition expects a valid Iglu-format URI as its path; got: ${err}")
      case Success(key) => key.toPath
    }
  }
  
  def toTuple(partition: String, tupleEntry: TupleEntry): Unit =
    throw new RuntimeException("ShreddedPartition's toTuple for reading not implemented")
}
