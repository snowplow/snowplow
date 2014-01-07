 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. with significant
 * portions copyright 2012-2014 Amazon.
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

package com.snowplowanalytics.kinesis.consumer

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{
  IRecordProcessor,
  IRecordProcessorFactory
}

class RecordProcessorFactory(config: KinesisConsumerConfig)
    extends IRecordProcessorFactory {
  @Override
  def createProcessor: IRecordProcessor = {
    return new RecordProcessor(config);
  }
}
