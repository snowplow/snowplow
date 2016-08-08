/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch
package clients

/**
 * Abstract class for Elasticsearch clients
 */
abstract class ElasticsearchSender extends IElasticsearchSender {

  /**
   * Period between retrying sending events to Elasticsearch
   *
   * @param sleepTime Length of time between tries
   */
  protected def sleep(sleepTime: Long): Unit = {
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }
  }
}
