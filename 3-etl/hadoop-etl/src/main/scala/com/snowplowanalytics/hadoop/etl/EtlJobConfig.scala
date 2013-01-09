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

// Scalding
import com.twitter.scalding.Args

// This project
import loaders.CollectorLoader

/**
 * Module to handle configuration for
 * the SnowPlowEtlJob
 */
object EtlJobConfig {
	
	/**
	 * Loads the Config from the Scalding
	 * job's supplied Args.
	 *
	 * TODO: currently throws a mix of
	 * Scalding exceptions (from required)
	 * plus FatalEtlException. Let's
	 * change it to return a validation or
	 * similar.
	 *
   * TODO: decide if we want to keep
   * ContinueOnUnexpecteError.
   *
	 * @param args The arguments to parse
	 * @return the configuration, in a
	 *         case class
	 */
	def loadConfigFrom(args: Args): EtlJobConfig = {

		val in  = args required "CLOUDFRONT_LOGS"
		val out = args required "EVENTS_TABLE"
	  val continue = {
	  	val c = args required "CONTINUE_ON"
	  	c == "1"
	  }
  	
  	val loader = {
	    val cf = args required "COLLECTOR_FORMAT"
	    CollectorLoader.getLoader(cf) getOrElse {
	      throw new EtlFatalException("collector_format '%s' not supported" format cf)
	    }
	  }

	  EtlJobConfig(
			inFolder = in,
			outFolder = out,
			collectorLoader = loader,
			continueOnUnexpectedError = continue)
	}
}
	
/**
 * The configuration for the
 * SnowPlowEtlJob. 
 *
 * TODO: decide if we want to keep
 * ContinueOnUnexpecteError.
 */
case class EtlJobConfig(
	val inFolder: String,
	val outFolder: String,
	val collectorLoader: CollectorLoader,
	val continueOnUnexpectedError: Boolean)