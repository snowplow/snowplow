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

import java.sql.DriverManager

import com.snowplowanalytics.snowplow.storage.kinesis.Redshift.EmitterInput

import scala.collection.JavaConverters._

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

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class RedshiftEmitter(config: KinesisConnectorConfiguration, props: Properties, badSink: ISink) extends IEmitter[ EmitterInput ] {

  val log = LogFactory.getLog(classOf[RedshiftEmitter])

  val redshift = DriverManager.getConnection(props.getProperty("redshift_url"), props.getProperty("redshift_username"), props.getProperty("redshift_password"))
  val emptyList = List[EmitterInput]()

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

    val values = new StringBuilder()
    buffer.getRecords.foreach{ record =>
      values.append(record._1).append(",")
    }
    if (values.endsWith(",")) values.setLength(values.length-1)
    val query: String = s"insert into ${props.getProperty("redshift_table")} " +
      s"(app_id,platform,etl_tstamp,collector_tstamp,dvce_tstamp,event,event_id,txn_id,name_tracker,v_tracker,v_collector,v_etl,user_id,user_ipaddress,user_fingerprint,domain_userid,domain_sessionidx,network_userid,geo_country,geo_region,geo_city,geo_zipcode,geo_latitude,geo_longitude,geo_region_name,ip_isp,ip_organization,ip_domain,ip_netspeed,page_url,page_title,page_referrer,page_urlscheme,page_urlhost,page_urlport,page_urlpath,page_urlquery,page_urlfragment,refr_urlscheme,refr_urlhost,refr_urlport,refr_urlpath,refr_urlquery,refr_urlfragment,refr_medium,refr_source,refr_term,mkt_medium,mkt_source,mkt_term,mkt_content,mkt_campaign,contexts,se_category,se_action,se_label,se_property,se_value,unstruct_event,tr_orderid,tr_affiliation,tr_total,tr_tax,tr_shipping,tr_city,tr_state,tr_country,ti_orderid,ti_sku,ti_name,ti_category,ti_price,ti_quantity,pp_xoffset_min,pp_xoffset_max,pp_yoffset_min,pp_yoffset_max,useragent,br_name,br_family,br_version,br_type,br_renderengine,br_lang,br_features_pdf,br_features_flash,br_features_java,br_features_director,br_features_quicktime,br_features_realplayer,br_features_windowsmedia,br_features_gears,br_features_silverlight,br_cookies,br_colordepth,br_viewwidth,br_viewheight,os_name,os_family,os_manufacturer,os_timezone,dvce_type,dvce_ismobile,dvce_screenwidth,dvce_screenheight,doc_charset,doc_width,doc_height,tr_currency,tr_total_base,tr_tax_base,tr_shipping_base,ti_currency,ti_price_base,base_currency,geo_timezone,augur_did,augur_uid,sink_tstamp) " +
      s"values $values"
    try {
      val rows = redshift.createStatement().executeUpdate(query)
      if (rows != 0) {
        log.info(s"Stored $rows rows in Redshift")
      }
    }
    catch {
      case e:Throwable =>
        log.error(query)
        throw e
    }
    emptyList
  }

  /**
   * Closes the client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown() {
    redshift.close()
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
      val output = compact(render(("line" -> record._1) ~ ("errors" -> record._2.swap.getOrElse(Nil))))
      badSink.store(output, Some("key"), false)
    }
  }
}
