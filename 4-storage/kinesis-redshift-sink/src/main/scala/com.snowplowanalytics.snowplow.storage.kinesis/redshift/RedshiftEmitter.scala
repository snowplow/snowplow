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

import com.snowplowanalytics.snowplow.storage.kinesis.Redshift.EmitterInput
import org.postgresql.ds.PGPoolingDataSource

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
import scala.language.implicitConversions

object RedshiftEmitter {
  var redshiftDataSource:PGPoolingDataSource = null
}

/**
 * Emitter for flushing Kinesis event data to S3.
 *
 * Once the buffer is full, the emit function is called.
 */
class RedshiftEmitter(config: KinesisConnectorConfiguration, props: Properties, badSink: ISink) extends IEmitter[ EmitterInput ] {
  val DEFAULT_BATCH_SIZE = 200
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

  class RedshiftOps(s: String) {
    def rsBoolean: Boolean = {
      if (s == null) throw new IllegalArgumentException("string is null")
      s.toLowerCase match {
        case "1" | "true" | "yes" => true
        case _ => false
      }
    }
  }
  implicit def redshiftOps(s: String): RedshiftOps = new RedshiftOps(s)

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

    val table = props.getProperty("redshift_table")
    val redshift = RedshiftEmitter.redshiftDataSource.getConnection
    try {
      val stat = redshift.prepareStatement(s"insert into $table " +
        "(app_id,platform,etl_tstamp,collector_tstamp,dvce_tstamp,event,event_id,txn_id,name_tracker,v_tracker,v_collector,v_etl,user_id," +
        "user_ipaddress,user_fingerprint,domain_userid,domain_sessionidx,network_userid,geo_country,geo_region,geo_city,geo_zipcode,geo_latitude," +
        "geo_longitude,geo_region_name,ip_isp,ip_organization,ip_domain,ip_netspeed,page_url,page_title,page_referrer,page_urlscheme,page_urlhost," +
        "page_urlport,page_urlpath,page_urlquery,page_urlfragment,refr_urlscheme,refr_urlhost,refr_urlport,refr_urlpath,refr_urlquery,refr_urlfragment," +
        "refr_medium,refr_source,refr_term,mkt_medium,mkt_source,mkt_term,mkt_content,mkt_campaign,contexts,se_category,se_action,se_label,se_property," +
        "se_value,unstruct_event,tr_orderid,tr_affiliation,tr_total,tr_tax,tr_shipping,tr_city,tr_state,tr_country,ti_orderid,ti_sku,ti_name,ti_category,ti_price," +
        "ti_quantity,pp_xoffset_min,pp_xoffset_max,pp_yoffset_min,pp_yoffset_max,useragent,br_name,br_family,br_version,br_type,br_renderengine,br_lang,br_features_pdf," +
        "br_features_flash,br_features_java,br_features_director,br_features_quicktime,br_features_realplayer,br_features_windowsmedia,br_features_gears," +
        "br_features_silverlight,br_cookies,br_colordepth,br_viewwidth,br_viewheight,os_name,os_family,os_manufacturer,os_timezone,dvce_type,dvce_ismobile," +
        "dvce_screenwidth,dvce_screenheight,doc_charset,doc_width,doc_height,tr_currency,tr_total_base,tr_tax_base,tr_shipping_base,ti_currency,ti_price_base," +
        "base_currency,geo_timezone,augur_did,augur_uid,sink_tstamp) " +
        "values (?,?,?,?,?, ?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?)")
      buffer.getRecords.sliding(DEFAULT_BATCH_SIZE).foreach { records =>
        records.foreach { record =>
          stat.setString(1, record._1(0))
          stat.setString(2, record._1(1))
          stat.setTimestamp(3, if (record._1(2) == null) null else Timestamp.valueOf(record._1(2)))
          stat.setTimestamp(4, if (record._1(3) == null) null else Timestamp.valueOf(record._1(3)))
          stat.setTimestamp(5, if (record._1(4) == null) null else Timestamp.valueOf(record._1(4)))
          stat.setString(6, record._1(5))
          stat.setString(7, record._1(6))
          if (record._1(7) == null) stat.setNull(8, Types.INTEGER) else stat.setInt(8, record._1(7).toInt)
          stat.setString(9, record._1(8))
          stat.setString(10, record._1(9))
          stat.setString(11, record._1(10))
          stat.setString(12, record._1(11))
          stat.setString(13, record._1(12))
          stat.setString(14, record._1(13))
          stat.setString(15, record._1(14))
          stat.setString(16, record._1(15))
          if (record._1(16) == null) stat.setNull(17, Types.INTEGER) else stat.setInt(17, record._1(16).toInt)
          stat.setString(18, record._1(17))
          stat.setString(19, record._1(18))
          stat.setString(20, record._1(19))
          stat.setString(21, record._1(20))
          stat.setString(22, record._1(21))
          if (record._1(22) == null) stat.setNull(23, Types.DOUBLE) else stat.setDouble(23, record._1(22).toDouble)
          if (record._1(23) == null) stat.setNull(24, Types.DOUBLE) else stat.setDouble(24, record._1(23).toDouble)
          stat.setString(25, record._1(24))
          stat.setString(26, record._1(25))
          stat.setString(27, record._1(26))
          stat.setString(28, record._1(27))
          stat.setString(29, record._1(28))
          stat.setString(30, record._1(29))
          stat.setString(31, record._1(30))
          stat.setString(32, record._1(31))
          stat.setString(33, record._1(32))
          stat.setString(34, record._1(33))
          if (record._1(34) == null) stat.setNull(35, Types.INTEGER) else stat.setInt(35, record._1(34).toInt) // page_urlport
          stat.setString(36, record._1(35))
          stat.setString(37, record._1(36))
          stat.setString(38, record._1(37))
          stat.setString(39, record._1(38))
          stat.setString(40, record._1(39))
          if (record._1(40) == null) stat.setNull(41, Types.INTEGER) else stat.setInt(41, record._1(40).toInt) // refr_urlport
          stat.setString(42, record._1(41))
          stat.setString(43, record._1(42))
          stat.setString(44, record._1(43))
          stat.setString(45, record._1(44))
          stat.setString(46, record._1(45))
          stat.setString(47, record._1(46))
          stat.setString(48, record._1(47))
          stat.setString(49, record._1(48))
          stat.setString(50, record._1(49))
          stat.setString(51, record._1(50))
          stat.setString(52, record._1(51))
          stat.setString(53, record._1(52))
          stat.setString(54, record._1(53))
          stat.setString(55, record._1(54))
          stat.setString(56, record._1(55))
          stat.setString(57, record._1(56))
          if (record._1(57) == null) stat.setNull(58, Types.DOUBLE) else stat.setDouble(58, record._1(57).toDouble) // se_value
          stat.setString(59, record._1(58))
          stat.setString(60, record._1(59))
          stat.setString(61, record._1(60))
          import java.math.{BigDecimal => BD}
          if (record._1(61) == null) stat.setNull(62, Types.DECIMAL) else stat.setBigDecimal(62, new BD(record._1(61))) // tr_total
          if (record._1(62) == null) stat.setNull(63, Types.DECIMAL) else stat.setBigDecimal(63, new BD(record._1(62))) //
          if (record._1(63) == null) stat.setNull(64, Types.DECIMAL) else stat.setBigDecimal(64, new BD(record._1(63))) //
          stat.setString(65, record._1(64))
          stat.setString(66, record._1(65))
          stat.setString(67, record._1(66))
          stat.setString(68, record._1(67))
          stat.setString(69, record._1(68))
          stat.setString(70, record._1(69))
          stat.setString(71, record._1(70))
          if (record._1(71) == null) stat.setNull(72, Types.DECIMAL) else stat.setBigDecimal(72, new BD(record._1(71))) //
          if (record._1(72) == null) stat.setNull(73, Types.INTEGER) else stat.setInt(73, record._1(72).toInt) //
          if (record._1(73) == null) stat.setNull(74, Types.INTEGER) else stat.setInt(74, record._1(73).toInt) //
          if (record._1(74) == null) stat.setNull(75, Types.INTEGER) else stat.setInt(75, record._1(74).toInt) //
          if (record._1(75) == null) stat.setNull(76, Types.INTEGER) else stat.setInt(76, record._1(75).toInt) //
          if (record._1(76) == null) stat.setNull(77, Types.INTEGER) else stat.setInt(77, record._1(76).toInt) // pp_yoffset
          stat.setString(78, record._1(77))
          stat.setString(79, record._1(78))
          stat.setString(80, record._1(79))
          stat.setString(81, record._1(80))
          stat.setString(82, record._1(81))
          stat.setString(83, record._1(82))
          stat.setString(84, record._1(83))
          if (record._1(84) == null) stat.setNull(85, Types.BOOLEAN) else stat.setBoolean(85, record._1(84).rsBoolean) //
          if (record._1(85) == null) stat.setNull(86, Types.BOOLEAN) else stat.setBoolean(86, record._1(85).rsBoolean) //
          if (record._1(86) == null) stat.setNull(87, Types.BOOLEAN) else stat.setBoolean(87, record._1(86).rsBoolean) //
          if (record._1(87) == null) stat.setNull(88, Types.BOOLEAN) else stat.setBoolean(88, record._1(87).rsBoolean) //
          if (record._1(88) == null) stat.setNull(89, Types.BOOLEAN) else stat.setBoolean(89, record._1(88).rsBoolean) //
          if (record._1(89) == null) stat.setNull(90, Types.BOOLEAN) else stat.setBoolean(90, record._1(89).rsBoolean) //
          if (record._1(90) == null) stat.setNull(91, Types.BOOLEAN) else stat.setBoolean(91, record._1(90).rsBoolean) //
          if (record._1(91) == null) stat.setNull(92, Types.BOOLEAN) else stat.setBoolean(92, record._1(91).rsBoolean) //
          if (record._1(92) == null) stat.setNull(93, Types.BOOLEAN) else stat.setBoolean(93, record._1(92).rsBoolean) //
          if (record._1(93) == null) stat.setNull(94, Types.BOOLEAN) else stat.setBoolean(94, record._1(93).rsBoolean) // br_cookies
          stat.setString(95, record._1(94))
          if (record._1(95) == null) stat.setNull(96, Types.INTEGER) else stat.setInt(96, record._1(95).toInt) //
          if (record._1(96) == null) stat.setNull(97, Types.INTEGER) else stat.setInt(97, record._1(96).toInt) // br_viewheight
          stat.setString(98, record._1(97))
          stat.setString(99, record._1(98))
          stat.setString(100, record._1(99))
          stat.setString(101, record._1(100))
          stat.setString(102, record._1(101))
          if (record._1(102) == null) stat.setNull(103, Types.BOOLEAN) else stat.setBoolean(103, record._1(102).rsBoolean) // dvce_ismobile
          if (record._1(103) == null) stat.setNull(104, Types.INTEGER) else stat.setInt(104, record._1(103).toInt) //
          if (record._1(104) == null) stat.setNull(105, Types.INTEGER) else stat.setInt(105, record._1(104).toInt) //
          stat.setString(106, record._1(105))
          if (record._1(106) == null) stat.setNull(107, Types.INTEGER) else stat.setInt(107, record._1(106).toInt) //
          if (record._1(107) == null) stat.setNull(108, Types.INTEGER) else stat.setInt(108, record._1(107).toInt) //
          stat.setString(109, record._1(108)) // tr_currency
          if (record._1(109) == null) stat.setNull(110, Types.DECIMAL) else stat.setBigDecimal(110, new BD(record._1(109))) //
          if (record._1(110) == null) stat.setNull(111, Types.DECIMAL) else stat.setBigDecimal(111, new BD(record._1(110))) //
          if (record._1(111) == null) stat.setNull(112, Types.DECIMAL) else stat.setBigDecimal(112, new BD(record._1(111))) //
          stat.setString(113, record._1(112)) //
          if (record._1(113) == null) stat.setNull(114, Types.DECIMAL) else stat.setBigDecimal(114, new BD(record._1(113))) // ti_price_base
          stat.setString(115, record._1(114)) //
          stat.setString(116, record._1(115)) // geo_timezone
          stat.setString(117, record._1(116)) // device_id
          stat.setString(118, record._1(117)) // user_id
          stat.setTimestamp(119, if (record._1(118) == null) null else Timestamp.valueOf(record._1(118))) // sink timestamp


          //        stat.setString(117, record._1(116)) //
          //        stat.setString(118, record._1(117)) //
          //        stat.setString(119, record._1(118)) //
          //        stat.setTimestamp(120, if (record._1(119) == null) null else Timestamp.valueOf(record._1(119)))
          //        stat.setString(121, record._1(120)) //
          //        stat.setTimestamp(122, if (record._1(121) == null) null else Timestamp.valueOf(record._1(121))) // refr_dvce_tstamp
          //        stat.setString(123, record._1(122)) //
          //        stat.setString(124, record._1(123)) //
          //        stat.setTimestamp(125, if (record._1(124) == null) null else Timestamp.valueOf(record._1(124))) // derived_tstamp

          stat.addBatch()
        }
        val count = stat.executeBatch().sum
        log.info(s"Inserted $count records into Redshift")
      }
    }
    catch {
      case s:BatchUpdateException =>
        log.error("Exception updating batch {}", s)
        log.error("Nested exception {}", s.getNextException)
        throw s
      case e:Throwable =>
        log.error("Exception updating batch {}", e)
        throw e
    }
    finally {
      try {
        redshift.close()
      } catch {
        case e:Throwable =>
          log.error("Unable to close redshift connection {}", e)
          throw e
      }
    }

    emptyList
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
