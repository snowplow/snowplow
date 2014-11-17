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

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchObject,
  ElasticsearchTransformer
}
import com.amazonaws.services.kinesis.model.Record

/**
 * Class to convert bad events to ElasticsearchObjects
 */
class BadEventTransformer(documentIndex: String, documentType: String) extends ElasticsearchTransformer[JsonRecord]
  with ITransformer[JsonRecord, ElasticsearchObject] {

  /**
   * Convert an Amazon Kinesis record to a JSON string
   *
   * @param record Byte array representation of a bad row string
   * @return JsonRecord containing JSON string for the event and no event_id
   */
  override def toClass(record: Record): JsonRecord =
    JsonRecord(new String(record.getData.array), None)

  /**
   * Convert a buffered bad event JSON to an ElasticsearchObject
   *
   * @param record JsonRecord containing a bad event JSON
   * @return An ElasticsearchObject
   */
  override def fromClass(record: JsonRecord): ElasticsearchObject =
    new ElasticsearchObject(documentIndex, documentType, "duplicated", record.json)

}
