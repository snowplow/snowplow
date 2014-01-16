/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.enrich.kinesis
package sinks

// Amazon
import com.amazonaws.auth._

object SinkFactory {
  def makeSink (config: KinesisEnrichConfig,
      kinesisProvider: AWSCredentialsProvider): ISink =
    config.sink match {
      case Sink.Kinesis =>
        val kinesisEnrichedSink = new KinesisSink(kinesisProvider)
        val successful = kinesisEnrichedSink.createAndLoadStream(
          config.enrichedOutStream,
          config.enrichedOutStreamShards
        )
        if (!successful) {
          throw new RuntimeException(
            "Error initializing or connecting to the stream."
          )
        }
        kinesisEnrichedSink
      case Sink.Stdouterr => new StdouterrSink
      case Sink.Test => null
    }
}
