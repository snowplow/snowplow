package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.snowplowanalytics.iglu.client.SchemaKey
import com.snowplowanalytics.snowplow.storage.kinesis.redshift.TableWriter
import org.apache.commons.logging.LogFactory
import scaldi.Injector
import scaldi.{Injector, Injectable}
import Injectable._

/**
 * Created by denismo on 21/09/15.
 * This class is meant to write the JSON events verbatim (without splitting into DB columns) as JSON, automatically creating the
 * required tables. The rationalle behind this is that it can simplify the introduction of custom schemas during the initial deployment
 * of Snowplow to a new website. Later such evens can be "shredded", or they can be directly queried using Redshift/Postgre JSON functions.
 * TODO: Complete the development, however easier approach was found - don't shred such events into a separate table, leave them in atomic.events
 *  as custom contexts
 */
class JsonDataTableWriter(dataSource:DataSource, schema: SchemaKey, table: String)(implicit injector: Injector)
  extends SchemaTableWriter(dataSource, schema, table)
{
  private val log = LogFactory.getLog(classOf[JsonDataTableWriter])

  val createSchemaTable =
    s"""
      |CREATE TABLE IF NOT EXISTS $table (
      |  -- Schema of this type
      |  schema_vendor   varchar(128)  encode runlength not null,
      |  schema_name     varchar(128)  encode runlength not null,
      |  schema_format   varchar(128)  encode runlength not null,
      |  schema_version  varchar(128)  encode runlength not null,
      |  -- Parentage of this type
      |  root_id         char(36)      encode raw not null,
      |  root_tstamp     timestamp     encode raw not null,
      |  ref_root        varchar(255)  encode runlength not null,
      |  ref_tree        varchar(1500) encode runlength not null,
      |  ref_parent      varchar(255)  encode runlength not null,
      |  app_id varchar(255) encode runlength not null,
      |  event_type varchar(255) encode runlength not null,
      |  json_data varchar(4096) encode lzo not null
      |)
      |  DISTSTYLE KEY
      |-- Optimized join to atomic.events
      |  DISTKEY (root_id)
      |  SORTKEY (root_tstamp);
    """.stripMargin

  override def onFlushToRedshift(flushCount: Int, providedCon: Option[Connection]) = {
    log.info(s"Flushing $table in Redshift")
    if (props.containsKey("simulateDB")) {
      Thread.sleep(20)
    } else {
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
          stat.execute(createSchemaTable + s" COPY $table FROM '$s3Manifest' " +
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
}
