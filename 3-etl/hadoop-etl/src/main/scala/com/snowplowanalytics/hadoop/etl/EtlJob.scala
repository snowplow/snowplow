/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._

// This project
import loaders._

/**
 * A fatal exception in our ETL.
 *
 * Will only be thrown if the ETL cannot
 * feasibly be run - do not try to catch it.
 */
case class FatalValidationException(msg: String) extends RuntimeException(msg)

/**
 * The SnowPlow ETL job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class EtlJob(args: Args) extends Job(args) {

  // Load configuration
  val etlConfig = EtlJobConfig.loadConfigFrom(args) match {
    case Success(config) => config
    case Failure(errors) => throw new FatalValidationException("OH MY GOD")
  }

  TextLine( etlConfig.inFolder )
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( Tsv( etlConfig.outFolder ) )

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}