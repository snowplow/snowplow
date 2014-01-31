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
package sources

/**
 * Source to allow the testing framework to enrich events
 * using the same methods from AbstractSource as the other
 * sources.
 */
class TestSource(config: KinesisEnrichConfig)
    extends AbstractSource(config) {

  /**
   * Never-ending processing loop over source stream.
   * Not supported for TestSource.
   */
  def run = {
    throw new RuntimeException("run() should not be called on TestSource")
  }
}
