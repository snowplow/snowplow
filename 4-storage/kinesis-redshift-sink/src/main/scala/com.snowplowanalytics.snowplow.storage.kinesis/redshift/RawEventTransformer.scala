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

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.ObjectMapper
import com.snowplowanalytics.snowplow.storage.kinesis.Redshift.{EmitterInput, ValidatedRecord}

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer

// Scalaz
import scalaz.Scalaz._
import scalaz._

class RawEventTransformer extends ITransformer[ ValidatedRecord, EmitterInput ] {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val timezone = TimeZone.getTimeZone("UTC")
  dateFormat.setTimeZone(timezone)
  override def toClass(record: Record): ValidatedRecord = {
    val recordByteArray = record.getData.array
    var fields = new String(recordByteArray, "UTF-8").split("\t")
    // Fix etl_tstamp
    import java.util.Date
    val date = new Date(java.lang.Long.valueOf(fields(2)))
    fields(2) = dateFormat.format(date)
    // Fix fields length
    while (fields.length < 116) {
      fields = fields ++ Array("")
    }
    // Extract augur - 118, 119
    fields = filterFields(fields)
    // Add sink timestamp
    fields = fields ++ Array(dateFormat.format(new Date()))
    val values = "(" + fields.map(f => if (f == "" || f == null) "NULL" else "'" + f + "'").mkString(",") + ")"
    (values, recordByteArray.success)
  }

  override def fromClass(record: EmitterInput) = record
  private lazy val Mapper = new ObjectMapper
  private val FieldCount = 108

  private object FieldIndexes { // 0-indexed
  val collectorTstamp = 3
    val eventId = 6
    val contexts = 52
    val unstructEvent = 58
    val network_userId = 17
    val user_fingerprint = 14
    val augur_did = 109
    val augur_user_id = 110
  }

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
      newFields(FieldIndexes.contexts) = null
      newFields(FieldIndexes.unstructEvent) = null
      newFields(FieldIndexes.network_userId) = null
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
