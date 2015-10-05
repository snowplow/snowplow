/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.kinesis.redshift

// AWS libs


import java.util.Properties

import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.ObjectMapper
import com.snowplowanalytics.iglu.client.Resolver
import org.joda.time.DateTimeZone

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer

// Scalaz
import scalaz.Scalaz._
import scalaz._
import scaldi.{Injector, Injectable}
import Injectable._

object FieldIndexes { // 0-indexed
  val collectorTstamp = 3
  val appId = 0
  val eventId = 6
  val contexts = 52
  val unstructEvent = 58
  val derived_contexts = 122
  val network_userId = 17
  val user_fingerprint = 14
  val augur_did = 109
  val augur_user_id = 110
}

class RawEventTransformer(implicit injector: Injector) extends ITransformer[ ValidatedRecord, EmitterInput ] {
  private val props = inject[Properties]
  override def toClass(record: Record): ValidatedRecord = {
    val recordByteArray = record.getData.array
    var fields = new String(recordByteArray, "UTF-8").split("\t", -1)
    // Fix etl_tstamp
    import org.joda.time.DateTime
    // Fix fields length
    while (fields.length < 125) {
      fields = fields ++ Array("")
    }
    if (props.containsKey("filterFields")) {
      // Extract augur - 126, 127
      fields = filterFields(fields)
      // Add sink timestamp 128
      fields = fields ++ Array(DateTime.now(DateTimeZone.forID("UTC")).toString("yyyy-MM-dd HH:mm:ss.SSS"))
    }
    val values = fields.map(f => if (f == "" || f == null) null else f)
    (values, recordByteArray.success)
  }

  override def fromClass(record: EmitterInput) = record
  private lazy val Mapper = new ObjectMapper
  private val FieldCount = 128

  def extractAugur(fields: Array[String]): Option[(String, String)] = {
    val contexts = fields(FieldIndexes.contexts)
    val unstruct = fields(FieldIndexes.unstructEvent)

    val nodes = List(contexts, unstruct).flatMap { json =>
      if (json != null && json != "") {
        try {
          val contextsJson = Mapper.readTree(json)
          val dataNode = contextsJson.at("/data")
          if (!dataNode.isMissingNode) {
            dataNode.isArray match {
              case true => Range(0, dataNode.size).map(index => dataNode.get(index))
              case false => List(dataNode)
            }
          } else {
            List()
          }
        }
        catch {
          case t: Throwable =>
            t.printStackTrace()
            List()
        }
      } else {
        List()
      }
    }

    nodes.flatMap { node =>
      val device_id = node.at("/data/augurDID")
      val user_id = node.at("/data/augurUID")
      (device_id.isMissingNode, user_id.isMissingNode) match {
        case (true, _) => None
        case (false, true) => Some(device_id.textValue(), null)
        case (false, false) => Some(device_id.textValue(), user_id.textValue())
      }
    }.headOption
  }

  def filterFields(fields: Array[String]): Array[String] = {
    try {
      val newFields = fields.clone()
//      newFields(FieldIndexes.contexts) = null
//      newFields(FieldIndexes.unstructEvent) = null
//      newFields(FieldIndexes.derived_contexts) = null
      // network_userid is used for authentication of realtime so we need to keep it
//      newFields(FieldIndexes.network_userId) = null
      newFields(FieldIndexes.user_fingerprint) = null
      val deviceDetails = extractAugur(fields)
      if (deviceDetails.isDefined) {
        val (device_id, user_id) = deviceDetails.get
        newFields ++ Array(device_id, user_id)
      } else {
        newFields ++ Array(null, null)
      }
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        fields ++ Array(null, null)
    }
  }

}
