 /*
  * Copyright (c) 2014 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

// Scala
import collection.JavaConversions._

// Amazon Kinesis
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Google BigQuery
import com.google.api.services.bigquery.model.TableRow

/**
 * Class to send records to BigQuery
 */
class SnowplowBigqueryEmitter(configuration: KinesisConnectorConfiguration)
extends IEmitter[IntermediateRecord]{

 def emit(x$1: com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer[IntermediateRecord]): java.util.List[IntermediateRecord] = ???

 def fail(x$1: java.util.List[IntermediateRecord]): Unit = ???

 def shutdown(): Unit = ???

}
