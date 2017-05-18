/*
 * Copyright (c) 2012 Twitter, Inc.
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
package com.snowplowanalytics.hadoop.scalding

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding.{JsonLine => StandardJsonLine, _}

// Cascading
import cascading.tuple.Fields
import cascading.tap.SinkMode

object JsonLine {
  def apply(p: String, fields: Fields = Fields.ALL) = new JsonLine(p, fields)
}
class JsonLine(p: String, fields: Fields) extends StandardJsonLine(p, fields, SinkMode.REPLACE) {
  // We want to test the actual tranformation here.
  override val transformInTest = true
}

class SnowplowBadRowsJobSpec extends Specification {
  import Dsl._

  "A SnowplowBadRows job" should {
    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowBadRowsJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(JsonLine("inputFile", ('line, 'errors)), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":["Line does not match CloudFront header or data row formats"]}\n""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "just extract the line" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish
  }
}
