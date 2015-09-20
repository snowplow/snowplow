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

import java.sql.{BatchUpdateException, Timestamp, Types}

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.EmitterInput
import org.postgresql.ds.PGPoolingDataSource

import scala.collection.JavaConverters._
import scala.collection.mutable
import scalaz.Validation

// Java libs
import java.util.Properties

// Java lzo

// Elephant bird

// Logging
import org.apache.commons.logging.LogFactory

// AWS libs

// AWS Kinesis connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.{KinesisConnectorConfiguration, UnmodifiableBuffer}

// Scala
import scala.collection.JavaConversions._

// Scalaz

// json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.sinks._
import scala.language.implicitConversions

object RedshiftEmitter {
  var redshiftDataSource:PGPoolingDataSource = null
}

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class RedshiftEmitter(config: KinesisConnectorConfiguration, badSink: ISink)(implicit resolver:Resolver, props: Properties) extends IEmitter[ EmitterInput ] {
  val log = LogFactory.getLog(classOf[RedshiftEmitter])

  {
    synchronized {
      if (RedshiftEmitter.redshiftDataSource == null) {
        val ds = new PGPoolingDataSource()
        ds.setUrl(props.getProperty("redshift_url"))
        ds.setUser(props.getProperty("redshift_username"))
        ds.setPassword(props.getProperty("redshift_password"))
        RedshiftEmitter.redshiftDataSource = ds
      }
    }
  }
  val emptyList = List[EmitterInput]()

  var shredder: InstantShredder = null

  /**
   * Reads items from a buffer and saves them to s3.
   *
   * This method is expected to return a List of items that
   * failed to be written out to Redshift, which will be sent to
   * a Kinesis stream for bad events.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation
   */
  override def emit(buffer: UnmodifiableBuffer[ EmitterInput ]): java.util.List[ EmitterInput ] = {
    log.info(s"Flushing buffer with ${buffer.getRecords.size} records.")
    val errors: mutable.MutableList[EmitterInput] = new mutable.MutableList[EmitterInput]
    try {
      if (shredder == null) {
        implicit val kinesisConfig = config
        shredder = new InstantShredder(RedshiftEmitter.redshiftDataSource)
      }
      implicit val dataSource = RedshiftEmitter.redshiftDataSource
      buffer.getRecords.foreach { record =>
        try {
          shredder.shred(record._1)
        }
        catch {
          case s:BatchUpdateException =>
            s.printStackTrace()
            if (s.getNextException != null) s.getNextException.printStackTrace()
            log.error("Exception updating batch", s)
            log.error("Nested exception", s.getNextException)
            errors.add(record)
          case t:Throwable =>
            t.printStackTrace()
            log.error("Exception shredding record", t)
            errors.add(record)
        }
      }
    }
    catch {
      case s:BatchUpdateException =>
        s.printStackTrace()
        if (s.getNextException != null) s.getNextException.printStackTrace()
        log.error("Exception updating batch", s)
        log.error("Nested exception", s.getNextException)
        throw s
      case t:Throwable =>
        t.printStackTrace()
        log.error("Exception flushing", t)
        throw t
    }

    errors
  }

  /**
   * Closes the client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown() {
  }

  /**
   * Sends records which fail deserialization or compression
   * to Kinesis with an error message
   *
   * @param records List of failed records to send to Kinesis
   */
  override def fail(records: java.util.List[ EmitterInput ]) {
    records.asScala.foreach { record =>
      log.warn(s"Record failed: $record")
      log.info("Sending failed record to Kinesis")
      val output = compact(render(("line" -> record._1.mkString("\t")) ~ ("errors" -> record._2.swap.getOrElse(Nil))))
      badSink.store(output, Some("key"), false)
    }
  }
}
