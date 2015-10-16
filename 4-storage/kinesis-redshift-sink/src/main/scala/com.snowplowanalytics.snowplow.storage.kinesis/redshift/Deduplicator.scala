package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.collection.JavaConverters._
import scalaz.Validation

/**
 * Created by denismo on 15/10/15.
 */
object Deduplicator {
  def deduplicate(records: java.util.List[EmitterInput]) : Iterable[EmitterInput] = {
    records.asScala.groupBy(_._1(FieldIndexes.eventId)).flatMap(group => deduplicateSimilar(group._2))
  }
  def deduplicate(records: Seq[Array[String]]) : Iterable[Array[String]] = {
    records.groupBy(_(FieldIndexes.eventId)).flatMap(group => deduplicateSimilar(group._2))
  }

  def recordHash(fields:Array[String]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    for (index <- List(FieldIndexes.appId, FieldIndexes.eventId, FieldIndexes.dvce_tstamp, FieldIndexes.event, FieldIndexes.platform)) {
      md.update(fields(index).getBytes("UTF-8"))
    }
    Hex.encodeHexString(md.digest())
  }

  val tstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
  def deduplicateSimilar(records: mutable.Buffer[EmitterInput]) : Seq[EmitterInput] = {
    // Records are grouped by eventId
    // 1. If they all match by hash -> keep only the latest, they are duplicates of the same event
    // 2. If they don't match by hash -> remove all, they should not have the same eventId
    if (records.nonEmpty) {
      if (records.map(record => recordHash(record._1)).distinct.size > 1) {
        // Different hashes
        Seq.empty
      } else {
        List(records.map(record => (tstampFormat.parseDateTime(record._1(FieldIndexes.collectorTstamp)), record))
          .sortWith { (group1, group2) =>
            group1._1.compareTo(group2._1) > 0 // sort descending - most recent in head
          }.head._2) // more than one item - head is safe
      }
    } else {
      records
    }
  }
  def deduplicateSimilar(records: Seq[Array[String]]) : Seq[Array[String]] = {
    // Records are grouped by eventId
    // 1. If they all match by hash -> keep only the latest, they are duplicates of the same event
    // 2. If they don't match by hash -> remove all, they should not have the same eventId
    if (records.nonEmpty) {
      if (records.map(recordHash).distinct.size > 1) {
        // Different hashes
        Seq.empty
      } else {
        List(records.map(record => (tstampFormat.parseDateTime(record(FieldIndexes.collectorTstamp)), record))
          .sortWith { (group1, group2) =>
            group1._1.compareTo(group2._1) > 0 // sort descending - most recent in head
          }.head._2) // more than one item - head is safe
      }
    } else {
      records
    }
  }
}
