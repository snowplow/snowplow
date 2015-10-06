package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.snowplowanalytics.iglu.client.SchemaKey
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.TableWriter
import org.apache.commons.logging.LogFactory
import scaldi.{Injector, Injectable}
import Injectable._

/**
 * Created by denismo on 18/09/15.
 */
class SchemaTableWriter(dataSource:DataSource, schema: SchemaKey, table: String)(implicit injector: Injector) extends BaseCopyTableWriter(dataSource, table) {
  private val log = LogFactory.getLog(classOf[SchemaTableWriter])
  val props = inject[Properties]
  val jsonPaths = {
    val propJsonPaths = props.getProperty("jsonPaths")
    val (major, _, _) = schema.getModelRevisionAddition.get
    val newName = schema.name.replaceAllLiterally(".","_").replaceAllLiterally("-","_").replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase
    val vendor = schema.vendor
    val path = s"$propJsonPaths/jsonpaths/$vendor/${newName}_$major.json"
    log.info(s"JsonPaths for $table is $path")
    path
  }
  log.info(s"Created schema table writer for $table")
  override def onFlushToRedshift(flushCount: Int, providedCon: Option[Connection]) = {
    log.info(s"Flushing $table in Redshift")
    if (bufferFile != null && bufferFile.exists()) {
      val con = providedCon match {
        case Some(_con) => _con
        case None =>
          TableWriter.getConnection(dataSource)
      }
      val stat = con.createStatement()
      try {
        try {
          val accessKey = props.getProperty("s3AccessKey")
          val secretKey = props.getProperty("s3SecretKey")
          stat.execute(s"COPY $table FROM '$s3Manifest' " +
            s"CREDENTIALS 'aws_access_key_id=$accessKey;aws_secret_access_key=$secretKey' " +
            s"json '$jsonPaths' " +
            "MAXERROR 1000 EMPTYASNULL TRUNCATECOLUMNS  TIMEFORMAT 'auto' ACCEPTINVCHARS GZIP SSH;")
          //          log.info(s"Finished flushing $table into Redshift")
        }
        finally {
          stat.close()
        }
      }
      finally {
        providedCon match {
          case Some(_con) => ()
          case None => con.close()
        }
      }
  }
  }
  def flush(con: Connection) = {
//    val flushCount = beforeFlushToRedshift()
    onFlushToRedshift(-1, Some(con))
  }
  override def flush(): Unit = {}
}
