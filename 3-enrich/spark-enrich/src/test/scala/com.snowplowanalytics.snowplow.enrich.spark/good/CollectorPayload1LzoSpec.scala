/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich.spark
package good

import java.io.File

import scala.collection.JavaConverters._

import org.specs2.mutable.Specification

import CollectorPayload.thrift.model1.CollectorPayload

object CollectorPayload1LzoSpec {
  import EnrichJobSpec._

  val payloadData = "e=pp&page=Loading%20JSON%20data%20into%20Redshift%20-%20the%20challenges%20of%20quering%20JSON%20data%2C%20and%20how%20Snowplow%20can%20be%20used%20to%20meet%20those%20challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJkYXRhIjpbeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91cmlfcmVkaXJlY3QvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsidXJpIjoiaHR0cDovL3Nub3dwbG93YW5hbHl0aWNzLmNvbS8ifX1dLCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIn0=&dtm=1398762054889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%2FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%3A%2F%2Fsnowplowanalytics.com%2Fservices%2Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2013%2F11%2F20%2Floading-json-data-into-redshift%2F%23weaknesses"
  val collectorPayload = new CollectorPayload(
    "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
    "255.255.255.255",
    1381175274000L,
    "UTF-8",
    "collector"
  )
  collectorPayload.setHeaders(List("X-Forwarded-For: 123.123.123.123, 345.345.345.345").asJava)
  collectorPayload.setPath("/i")
  collectorPayload.setHostname("localhost")
  collectorPayload.setQuerystring(payloadData)
  collectorPayload.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36")
  collectorPayload.setNetworkUserId("8712a379-4bcb-46ee-815d-85f26540577f")

  val expected = List(
    "snowplowweb",
    "web",
    etlTimestamp,
    "2013-10-07 19:47:54.000",
    "2014-04-29 09:00:54.889",
    "page_ping",
    null, // We can't predict the event_id
    "612876",
    "cloudfront", // Tracker namespace
    "js-2.0.0",
    "collector",
    etlVersion,
    null, // No user_id set
    "123.123.123.x",
    "2071613637",
    "44082d3af0e30126",
    "2",
    "8712a379-4bcb-46ee-815d-85f26540577f",
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    "http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/#weaknesses",
    "Loading JSON data into Redshift - the challenges of quering JSON data, and how Snowplow can be used to meet those challenges",
    "http://snowplowanalytics.com/services/pipelines.html",
    "http",
    "snowplowanalytics.com",
    "80",
    "/blog/2013/11/20/loading-json-data-into-redshift/",
    null,
    "weaknesses",
    "http",
    "snowplowanalytics.com",
    "80",
    "/services/pipelines.html",
    null,
    null,
    "internal", // Internal referer
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    """{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"http://snowplowanalytics.com/"}}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""",
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event field empty
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    "0", // Page ping fields
    "1", //
    "64", //
    "935", //
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36", // previously "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome 31", // previously "Chrome"
    "Chrome",
    "31.0.1650.8",// previously "34.0.1847.131"
    "Browser",
    "WEBKIT",
    "fr",
    "1",
    "1",
    "1",
    "0",
    "1",
    "0",
    "0",
    "0",
    "0",
    "1",
    "24",
    "1279",
    "610",
    "Mac OS X",
    "Mac OS X",
    "Apple Inc.",
    "Europe/Berlin",
    "Computer",
    "0",
    "1280",
    "800",
    "UTF-8",
    "1279",
    "5614"
  )
}

/**
 * Test that a raw Thrift event is processed correctly.
 * See https://github.com/snowplow/snowplow/issues/538
 * Based on Apr2014CfLineSpec.
 */
class CollectorPayload1LzoSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "collector-payload1-lzo"
  sequential
  "A job which processes a RawThrift file containing 1 valid page view" should {
    val f = write("input", CollectorPayload1LzoSpec.collectorPayload)
    runEnrichJob(f.toString(), "thrift", "1", true, List("geo"), false, false, false, false)

    "correctly output 1 page view" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CollectorPayload1LzoSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(CollectorPayload1LzoSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }

  def write(tag: String, payload: CollectorPayload): File = {
    import com.twitter.elephantbird.mapreduce.io.ThriftWritable
    import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat
    import org.apache.hadoop.io.LongWritable
    val f = new File(System.getProperty("java.io.tmpdir"),
      s"snowplow-enrich-job-${tag}-${scala.util.Random.nextInt(Int.MaxValue)}")
    val rdd = spark.sparkContext.parallelize(Seq(payload))
      .map { e =>
        val writable = ThriftWritable.newInstance(classOf[CollectorPayload])
        writable.set(e)
        (new LongWritable(1L), writable)
      }
    LzoThriftBlockOutputFormat.setClassConf(classOf[CollectorPayload], hadoopConfig)
    rdd.saveAsNewAPIHadoopFile(
      f.toString(),
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[ThriftWritable[CollectorPayload]],
      classOf[LzoThriftBlockOutputFormat[ThriftWritable[CollectorPayload]]],
      hadoopConfig
    )
    f
  }
}
