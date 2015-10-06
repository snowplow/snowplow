package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.TableWriter
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.limiter._
import org.apache.commons.logging.LogFactory
import scaldi.Injector

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps
import scaldi.{Injector, Injectable}
import Injectable._

/**
 * Created by denismo on 18/09/15.
 */
class EventsTableWriter(dataSource:DataSource, table: String)(implicit injector: Injector) extends BaseCopyTableWriter(dataSource, table) {
  private val log = LogFactory.getLog(classOf[EventsTableWriter])
  var dependents: Set[SchemaTableWriter] = Set()

  val props = inject[Properties]
  val limiter: FlushLimiter = {
    if (props.containsKey("flushRatio") && props.containsKey("defaultCollectionTime")) {
      if (props.containsKey("batchSize")) {
        new OrLimiter(
          new RatioFlushLimiter(props.getProperty("flushRatio"), java.lang.Long.parseLong(props.getProperty("defaultCollectionTime")),
            java.lang.Long.parseLong(props.getProperty("maxCollectionTime"))),
          new SizeFlushLimiter(Integer.parseInt(props.getProperty("batchSize")))
        )
      } else {
        new RatioFlushLimiter(props.getProperty("flushRatio"), java.lang.Long.parseLong(props.getProperty("defaultCollectionTime")),
          java.lang.Long.parseLong(props.getProperty("maxCollectionTime")))
      }
    } else if (props.containsKey("batchSize")) {
      new SizeFlushLimiter(Integer.parseInt(props.getProperty("batchSize")))
    } else {
      throw new scala.RuntimeException("Need to specify either batchSize or numerator/denominator/minWriteTime for flush limiter")
    }
  }
  log.info(s"Created events table writer for $table")

  override def isFlushRequired: Boolean = limiter.isFlushRequired

  override def beforeFlushToRedshift():Int = {
    val res = super.beforeFlushToRedshift()

    dependents.foreach(_.beforeFlushToRedshift())

    res
  }

  override def write(value: Array[String]): Unit = {
    limiter.onRecord(value)
    super.write(value)
  }

  override def onFlushToRedshift(flushCount:Int, providedCon: Option[Connection]) = {
    val start = System.currentTimeMillis()
    log.info(s"Flushing $flushCount events $table to Redshift")
    val con = providedCon match {
      case Some(_con) => _con
      case None =>
        TableWriter.getConnection(dataSource)
    }
    val stat = con.createStatement()
    try {
      if (bufferFile != null && bufferFile.exists()) {
        try {
          val accessKey = props.getProperty("s3AccessKey")
          val secretKey = props.getProperty("s3SecretKey")
          stat.execute(s"COPY $table FROM '$s3Manifest' " +
            s"CREDENTIALS 'aws_access_key_id=$accessKey;aws_secret_access_key=$secretKey' " +
            "DELIMITER '\\t' MAXERROR 1000 EMPTYASNULL FILLRECORD TRUNCATECOLUMNS  TIMEFORMAT 'auto' ACCEPTINVCHARS GZIP  SSH;")
          log.info(s"Finished flushing events $table to Redshift")
        }
        finally {
          stat.close()
        }
      }
      dependents.foreach(_.flush(con))
      log.info("Committing events")
      con.commit()
      log.info("Committed events")
      limiter.flushed(start, System.currentTimeMillis(), flushCount)
    }
    catch {
      case e:Throwable =>
        e.printStackTrace()
        log.error("Exception flushing events to Redshift. Rolling back", e)
        con.rollback()
        throw e
    }
    finally {
      providedCon match {
        case Some(_con) => ()
        case None => con.close()
      }
    }
  }

  def addDependent(dependent: SchemaTableWriter) = {
    dependents += dependent
  }

  override def close() = {
    try {
      waitForPending(Some(s"Waiting for previous pending flush to complete before closing writer for $table"))
      flush()
      waitForPending(Some(s"Waiting for last pending flush to complete before closing writer for $table"))
    }
    catch {
      case e:Throwable =>
        e.printStackTrace()
        log.error("Exception performing final flush - ignoring", e)
    }
    try {
      dependents.foreach(_.close())
      super.close()
    }
    catch {
      case e:Throwable =>
        e.printStackTrace()
        log.error("Exception closing writers - ignoring", e)
    }
  }
}
