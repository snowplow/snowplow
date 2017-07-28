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

package com.snowplowanalytics
package snowplow.enrich
package kinesis
package sources


// Apache Commons
import org.apache.commons.codec.binary.Base64

// NSQ
import com.github.brainlag.nsq.NSQConsumer
import com.github.brainlag.nsq.lookup.DefaultNSQLookup
import com.github.brainlag.nsq.NSQMessage
import com.github.brainlag.nsq.callbacks.NSQMessageCallback

// Iglu
import iglu.client.Resolver

// Snowplow
import common.enrichments.EnrichmentRegistry

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
  * Source to decode raw events (in base64)
  * from NSQ.
  */
class NsqSource(config: KinesisEnrichConfig, igluResolver: Resolver, enrichmentRegistry: EnrichmentRegistry, tracker: Option[Tracker])
  extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {

  /**
    * Consumer will be started to wait new message.
    */
  def run = {

    val nsqCallback = new NSQMessageCallback {
      def message(msg: NSQMessage) = {
        val bytes = Base64.decodeBase64(msg.getMessage())
        enrichAndStoreEvents(List(bytes))
        //now mark the message as finished.
        msg.finished()
        //or you could requeue it, which indicates a failure and puts it back on the queue.
        //message.requeue();
      }
    }

    // use NSQLookupd
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(config.nsqHost, config.nsqlookupdPort)
    val consumer = new NSQConsumer(lookup, config.nsqGoodSourceTopicName, "channel", nsqCallback)
    consumer.start()
  }
}
