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
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchObject,
  ElasticsearchEmitter,
  ElasticsearchTransformer
}
import com.amazonaws.services.kinesis.model.Record

class SnowplowElasticsearchTransformer extends ElasticsearchTransformer[String]
  with ITransformer[String, ElasticsearchObject] {

  override def toClass(record: Record): String = {
    println(">>>>>>>>>>>>>>>> CALLING TOCLASS NOW")
    //throw new RuntimeException(record.toString)
    "todo"
  }

  override def fromClass(record: String): ElasticsearchObject  =  {
    println("---------------===============+++++++++++++++++++++=================------------------")
    println("---------------===============+++++++++++++++++++++=================------------------")
    println("---------------===============+++++++++++++++++++++=================------------------")
    println("---------------===============+++++++++++++++++++++=================------------------")
    new ElasticsearchObject("someindex", "sometype", "someid", """{"a":"b"}n""")
  }

}
