/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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

// Commons
import org.apache.commons.codec.binary.Base64
import java.nio.charset.StandardCharsets.UTF_8

class SnowplowEventRecoveryJobSpec extends Specification {
  import Dsl._

  "A SnowplowEventRecovery job" should {

    val identity = """
      function process(event, errors) {
        return arrayToTsv(tsvToArray(event));
      }
    """

    val changeMethod = """
      function process(event, errors) {
        var fields = tsvToArray(event);
        fields[5] = 'POST';
        return arrayToTsv(fields);
      }
    """

    val discardByError = """
      function process(event, errors) {
        if (errors.length == 1 && /schema not found/.test(errors[0])) {
          return event;
        } else {
          return null;
        }
      }
    """

    val changeQuerystring = """
      function process(event, errors) {
        var fields = tsvToArray(event);
        var querystringDict = parseQuerystring(fields[11]);
        querystringDict['tv'] = 'js-2.7.0';
        fields[11] = buildQuerystring(querystringDict);
        return arrayToTsv(fields);
      }
    """

    val decodeContexts = """
      function process(event, errors) {
        var fields = tsvToArray(event);
        var querystringDict = parseQuerystring(fields[11]);
        var contexts = querystringDict['cx'];
        fields[0] = new java.lang.String(org.apache.commons.codec.binary.Base64.decodeBase64(contexts));
        return arrayToTsv(fields);
      }
    """

    val fixMissingSchemasAndUrls = """
      function process(event, errors) {

          var failedUrl = false;

          for (var i=0; i<errors.length; i++) {
              var err = errors[i];
              if (isBadUrlError(err)) {
                  failedUrl = true;
              } else if (!isMissingSchemaError(err)) {
                  return null;
              }
          }

          if (failedUrl) {
              var fields = tsvToArray(event);
              fields[9] = 'http://www.placeholder.com';

              if (fields[5] == 'GET') {

                var querystring = parseQuerystring(fields[11]);
                querystring['url'] = 'http://www.placeholder.com';
                querystring['refr'] = 'http://www.placeholder.com';
                fields[11] = buildQuerystring(querystring);
                return arrayToTsv(fields);
              } else if (fields[5] == 'POST') {

                var postPosition = fields.length - 1;
                var decodedPost = decodeBase64(fields[postPosition]);
                var postJson = parseJson(decodedPost);
                for (var i=0;i<postJson.data.length;i++) {
                  postJson.data[i].url = 'http://www.placeholder.com';
                  postJson.data[i].refr = 'http://www.placeholder.com';
                }
                fields[postPosition] = encodeBase64(stringifyJson(postJson));
                return arrayToTsv(fields);
              } else {
                return null;
              }
          } else {
              return event;
          }
      }

      function isBadUrlError(err) {
          return /RFC 2396|could not be parsed by Netaporter|Unexpected error creating URI from string/.test(err);
      }

      function isMissingSchemaError(err) {
          return /Could not find schema with key/.test(err);
      }
    """

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(identity.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"},{"level":"error","message":"Unrecognized event [null]"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "extract the line" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(changeMethod.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"},{"level":"error","message":"Unrecognized event [null]"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "mutate the raw event" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tPOST\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(changeQuerystring.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"},{"level":"error","message":"Unrecognized event [null]"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "mutate the querystring" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=js-2.7.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(discardByError.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List(
        (0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"},{"level":"error","message":"Unrecognized event [null]"}]}"""),
        (1, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"schema not found"}]}"""),
        (2, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"schema not found"},{"level":"error","message":"Unrecognized event [null]"}]}""")
        ))
      .sink[String](Tsv("outputFile")) { buf =>
        "filter based on error messages" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(changeMethod.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List(
        (0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"}]}"""),
        (1, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":["Field [e]: [psp] is not a recognised event code"]}""")
        ))
      .sink[String](Tsv("outputFile")) { buf =>
        "accept both old-style and new-style bad row formats" in {
          buf.size must_== 2
          buf(0) must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tPOST\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
          buf(1) must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tPOST\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(decodeContexts.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"Field [e]: [psp] is not a recognised event code"},{"level":"error","message":"Unrecognized event [null]"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "use Java classes to decode contexts" in {
          buf.size must_== 1
          buf.head must be_==("{\"page\":{\"url\":\"analytics\"}}\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(fixMissingSchemasAndUrls.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://snowplowanalytics.com/analytics/customer-analytics/cohort-analysis.html\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fattribution.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fanalytics%252Fcustomer-analytics%252Fcohort-analysis.html\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001","errors":[{"level":"error","message":"could not be parsed by Netaporter"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "Fix up both page and referrer URLs in a GET" in {
          buf.size must_== 1
          buf.head must be_==("2014-04-28\t18:13:40\tJFK1\t831\t76.9.199.178\tGET\td3v6ndkyapxc2w.cloudfront.net\t/i\t200\thttp://www.placeholder.com\tMozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36\te=pp&page=Cohort%2520Analysis&pp_mix=0&pp_max=0&pp_miy=1999&pp_may=1999&cx=eyJwYWdlIjp7InVybCI6ImFuYWx5dGljcyJ9fQ&dtm=1398708819267&tid=319357&vp=1436x783&ds=1436x6689&vid=4&duid=d91cd3ae94725999&p=web&tv=2.0.0&fp=985410387&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%252FNew_York&tna=cloudfront&evn=com.snowplowanalytics&refr=http%3A%2F%2Fwww.placeholder.com&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&url=http%3A%2F%2Fwww.placeholder.com\t-\tHit\t1x0ytHOrKiCpOY9JW7UmBUm7_P1LNVgzZexm42vCShxJcUlfp8EMOw==\td3v6ndkyapxc2w.cloudfront.net\thttp\t1014\t0.001")
        }
      }
      .run
      .finish

    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowEventRecoveryJob")
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("inputFormat", "bad")
      .arg("script", new String(Base64.encodeBase64(fixMissingSchemasAndUrls.getBytes()), UTF_8))
      .source(MultipleTextLineFiles("inputFile"), List((0, """{"line":"2016-03-17\t18:04:29\t-\t-\t152.13.249.105\tPOST\t152.13.249.105\t/com.snowplowanalytics.snowplow/tp2\t200\thttp://www.placeholder.com\tMozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%3B+rv%3A43.0%29+Gecko%2F20100101+Firefox%2F43.0\t=&cv=clj-1.0.0-tom-0.2.0&nuid=afdf9acd-8e90-4d64-bede-a8f4c17c1871&url=http%3A%2F%2Fwww.placeholder.com&refr=http%3A%2F%2Fwww.placeholder.com\t-\t-\t-\tapplication%2Fjson%3B+charset%3DUTF-8\teyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9wYXlsb2FkX2RhdGEvanNvbnNjaGVtYS8xLTAtMiIsImRhdGEiOlt7ImUiOiJ1ZSIsInVlX3B4IjoiZXlKelkyaGxiV0VpT2lKcFoyeDFPbU52YlM1emJtOTNjR3h2ZDJGdVlXeDVkR2xqY3k1emJtOTNjR3h2ZHk5MWJuTjBjblZqZEY5bGRtVnVkQzlxYzI5dWMyTm9aVzFoTHpFdE1DMHdJaXdpWkdGMFlTSTZleUp6WTJobGJXRWlPaUpwWjJ4MU9tTnZiUzUwWld4dloybGpZV3h6ZVhOMFpXMXpMMkZrYVcxd2NtVnpjMmx2Ymk5cWMyOXVjMk5vWlcxaEx6RXRNQzB4SWl3aVpHRjBZU0k2ZXlKMFpXeHZaMmxqWVd4cFpDSTZJbVJqTm1NNU5tVTNMV05oTUdNdE5HUXpOaTFpTkRaa0xXRTJZamd4TURNeFpUYzJZeUlzSW5SbGMzUnBaQ0k2SW1GMGRHMXZZbWxzYVhSNUlpd2lkWE5sY21sa0lqb2lOREV3TmpJd05qazRNRGMwTWpRd09EQXpOQ0lzSW1ScGJXVnVjMmx2Ym5kcFpIUm9Jam9pTnpJNElpd2laR2x0Wlc1emFXOXVhR1ZwWjJoMElqb2lPVEFpTENKamJHbGphM1Z5YkNJNklpSXNJbU55WldGMGFYWmxhV1FpT2lJek9UZ3dNVE00T0NJc0ltRmtkbVp5WlhFaU9pSXdJaXdpWVhWamRHbHZibWxrSWpvaU1qRXpPVGMxTURRNE5ERTFPRFF6TkRZd09DSXNJbkJ5YVdObGNHRnBaQ0k2SWpNdU1ETXlPVFkzSWl3aVlXNWtjbTlwWkdsa0lqb2lJaXdpWVhCd2JHVnBaQ0k2SWlJc0luQjFZbXhwYzJobGNtbGtJam9pTVRjNU1Ea2lMQ0poWjJVaU9pSXdJaXdpY21WbWRYSnNJam9pYUhSMGNEb3ZMM2QzZHk1bFltRjVMbU52YlM4aUxDSnpaV3hzWlhKdFpXMXBaQ0k2SWpFNE1TSXNJbk5sYzNObWNtVnhJam9pTFRFaUxDSnphWFJsYVdRaU9pSTFOVFF4T0NJc0lucHBjR052WkdVaU9pSXlOelExTlNJc0ltTnBkSGtpT2lKSGNtVmxibk5pYjNKdklpd2lZMjkxYm5SeWVTSTZJbFZUSWl3aWRYTmxjbWx3SWpvaU1UVXlMakV6TGpJME9TNHdJaXdpWW1sa2NISnBZMlVpT2lJekxqUXhOVGcxTnlJc0ltTnlaV0YwYVhabGMybDZaU0k2SWpjeU9IZzVNQ0lzSW1SdGNISmxjM1J5YVdOMGFXOXVZWEJ3YkdsbGN5STZJblJ5ZFdVaUxDSnZjSFJ2ZFhSeVpYTjBjbWxqZEdsdmJtRndjR3hwWlhNaU9pSm1ZV3h6WlNJc0luSmxjWFZsYzNScFpDSTZJbU0wTlRSaU1qY3pMVFpoWkdRdE5HSm1OQzA0TmpnNUxUUmpOalU0WTJKa01EUTBNaUlzSW1OaGJYQmhhV2R1YVdRaU9pSXhNREkzTWpnMk1pSjlmWDAiLCJ0diI6ImpzLTIuNC4zIiwidG5hIjoiY28iLCJhaWQiOiJhZGltcHJlc3Npb25fYXR0bW9iaWxpdHkiLCJwIjoid2ViIiwidHoiOiJBbWVyaWNhL05ld19Zb3JrIiwibGFuZyI6ImVuLVVTIiwiY3MiOiJ3aW5kb3dzLTEyNTIiLCJmX3BkZiI6IjEiLCJmX3F0IjoiMCIsImZfcmVhbHAiOiIwIiwiZl93bWEiOiIwIiwiZl9kaXIiOiIwIiwiZl9mbGEiOiIxIiwiZl9qYXZhIjoiMCIsImZfZ2VhcnMiOiIwIiwiZl9hZyI6IjEiLCJyZXMiOiIxOTIweDEwODAiLCJjZCI6IjI0IiwiY29va2llIjoiMSIsImVpZCI6IjFhZDUyNjAwLTE0ZTQtNDI2Zi05NDkyLWY0ZWUzMDE3N2Y3ZSIsImR0bSI6IjE0NTgyMzczNjA4ODUiLCJ2cCI6IjcyOHg5MCIsImRzIjoiNzI4eDkxIiwidmlkIjoiMSIsImR1aWQiOiJkMzhiMmY2ODFiNWE3NjQ0IiwiZnAiOiIyOTgyNzIxMDYzIiwicmVmciI6Imh0dHA6Ly9hZC5kb3VibGVjbGljay5uZXQvTjYyNDUvYWRpL2ViYXkuZWJheXVzLnNlYXJjaC9idGY7dGNhdD0zNjQ2OTtrdz1ibXcrejQ7aXRlbXM9MTA1O2ljZT07Y2NvZGU9O3N6PTcyOHg5MDt1PWlfNDA2MDk4NDkyMDkyODg2NDMxOHxtXzU0Mzk1OTs7Y2F0PTM2NDY5O2NhdD02MDAwO2NhdD02MDAxO2NhdD02MDA2O3NlZz1oZmRzaWZld2Nta3djaXc7c2VnPXI1NjQ0O3NlZz1yNTY0NjtzZWc9cjU2NDk7dGlsZT00O290PTE7dW09ODtlYl90cms9NTQzOTU5O3ByPTI0O3hwPTMyO25wPTIyO3V6PTI3NDA1O2ZiaT0xO3NiaT0xMzkwNTtmYm89MTtzYm89MTM3Nzc7ZnNlPTMyMDczO3NzZT0wO2Z2aT0yODE7c3ZpPTEwOTY4O21kaWQ9MDUyMjEwMTgwMzUzMjUwNzU0NjE1NEFBQUFBQUFBQUE7Y2c9ODVhNzMyY2QxNTMwYTVmMTY0OTU4ODE1ZjljZmUwMDM7b3JkPTE0NTgyMzczNTkwNDQ7IiwidXJsIjoiaHR0cDovL3RlbHRhZy50ZWxvZ2ljYWwuY29tL2FkL2lmcmFtZT90ZXN0aWQ9YXR0bW9iaWxpdHkmZm9ybWF0PWh0bWwmdXNlcmlkPTQxMDYyMDY5ODA3NDI0MDgwMzQmY3JlYXRpdmVpZD0zOTgwMTM4OCZjcmVhdGl2ZXNpemU9NzI4eDkwJmNhbXBhaWduaWQ9MTAyNzI4NjImdHJhY2tlcj0xSEJBVV8yMDE2JmFkdmZyZXE9MCZhdWN0aW9uaWQ9MjEzOTc1MDQ4NDE1ODQzNDYwOCZwcmljZXBhaWQ9My4wMzI5NjcmYW5kcm9pZGlkPSZhcHBsZWlkPSZwdWJsaXNoZXJpZD0xNzkwOSZhZ2U9MCZyZWZ1cmw9aHR0cCUzQSUyRiUyRnd3dy5lYmF5LmNvbSUyRiZzZWxsZXJtZW1pZD0xODEmc2Vzc2ZyZXE9LTEmc2l0ZWlkPTU1NDE4JnppcGNvZGU9Mjc0NTUmY2l0eT1HcmVlbnNib3JvJmNvdW50cnk9VVMmdXNlcmlwPTE1Mi4xMy4yNDkuMCZiaWRwcmljZT0zLjQxNTg1NyZjYWNoZWJ1c3Rlcj0xNDU4MjM3MzYwJmNsaWNrPWh0dHAlM0ElMkYlMkZueW0xLmliLmFkbnhzLmNvbSUyRmNsaWNrJTNGT0lSRE9JUkRDRUFVcmtmaGVoUUdRQUFBQUFBQUFCeEFscXdHVnhEZUNFQ0wtMmF1ckZNTFFERE5PM3k2NjdFZFltZGhUWDR0X0RpdzctcFdBQUFBQUttSUh3QzFBQUFBaGdrQUFBSUFBQUFzVWw4QzBXTUFBQUFBQVFCVlUwUUFWVk5FQU5nQ1dnQkR2d0FBMktvQkFnVUFBUUFBQUFBQXVTQnN3d0FBQUFBLiUyRmNuZCUzRCUyNTIxc1FrbWRRaml4ZWtGRUt5a19SSVkwY2NCSUFBb2lvYk15Z1UuJTJGYm4lM0Q0MzczNiUyRnJlZmVycmVyJTNEaHR0cCUyNTNBJTI1MkYlMjUyRnd3dy5lYmF5LmNvbSUyNTJGJTJGY2xpY2tlbmMlM0RodHRwJTI1M0ElMjUyRiUyNTJGYWRjbGljay5nLmRvdWJsZWNsaWNrLm5ldCUyNTJGYWNsayUyNTNGc2ElMjUzREwlMjUyNmFpJTI1M0RDWUdTZXItX3FWb3ZXTTR5Y21BVGgzNUxZRHFhazFOc0ducGpObWJRQndJMjNBUkFCSUFCZ3lkYW9pcVNrbkJDQ0FSZGpZUzF3ZFdJdE5UTXpOVFEwT1RVMU5EUTRNamszT2NnQkNlQUNBS2dEQWFvRW1RRlAwRU5MeGN4RFB5VjdlMDZJQ1pWTVE3RXpQTm9vTzlZSFQzU20wbVBFR05XUUxwYm4zaGsxLWRpZXM1YndCd0VsSGluY1FfUDdqbzNHUHZvajdVWTN4cUtEbDhvaTdvY0s1aWtRbEJ4aGUxUkRNUUlzWGt5d0RyVGpvRC1oRGxJUndOWk5wS1hHaFhnRkNrbmo4TEZCUWNMT3gtQTdtaE01WHAzb2Rqbm5IRWhvOHJiWU05eW93YXRHeUZfWmZIX2VyRjFpZ08wTEhqRGdCQUdBQnR1VzBaQzcwUC13TGFBR0lkZ0hBQSUyNTI2bnVtJTI1M0QxJTI1MjZzaWclMjUzREFPRDY0XzNIcXNzS0x4NmFqeFZjVFJ6cjRaeVRyWnBLX2clMjUyNmNsaWVudCUyNTNEY2EtcHViLTUzMzU0NDk1NTQ0ODI5NzklMjUyNmFkdXJsJTI1M0QifV19","errors":[{"level":"error","message":"could not be parsed by Netaporter"}]}""")))
      .sink[String](Tsv("outputFile")) { buf =>
        "Fix up both page and referrer URLs in a POST" in {
          buf.size must_== 1
          buf.head must be_==("2016-03-17\t18:04:29\t-\t-\t152.13.249.105\tPOST\t152.13.249.105\t/com.snowplowanalytics.snowplow/tp2\t200\thttp://www.placeholder.com\tMozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%3B+rv%3A43.0%29+Gecko%2F20100101+Firefox%2F43.0\t=&cv=clj-1.0.0-tom-0.2.0&nuid=afdf9acd-8e90-4d64-bede-a8f4c17c1871&url=http%3A%2F%2Fwww.placeholder.com&refr=http%3A%2F%2Fwww.placeholder.com\t-\t-\t-\tapplication%2Fjson%3B+charset%3DUTF-8\teyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9wYXlsb2FkX2RhdGEvanNvbnNjaGVtYS8xLTAtMiIsImRhdGEiOlt7ImUiOiJ1ZSIsInVlX3B4IjoiZXlKelkyaGxiV0VpT2lKcFoyeDFPbU52YlM1emJtOTNjR3h2ZDJGdVlXeDVkR2xqY3k1emJtOTNjR3h2ZHk5MWJuTjBjblZqZEY5bGRtVnVkQzlxYzI5dWMyTm9aVzFoTHpFdE1DMHdJaXdpWkdGMFlTSTZleUp6WTJobGJXRWlPaUpwWjJ4MU9tTnZiUzUwWld4dloybGpZV3h6ZVhOMFpXMXpMMkZrYVcxd2NtVnpjMmx2Ymk5cWMyOXVjMk5vWlcxaEx6RXRNQzB4SWl3aVpHRjBZU0k2ZXlKMFpXeHZaMmxqWVd4cFpDSTZJbVJqTm1NNU5tVTNMV05oTUdNdE5HUXpOaTFpTkRaa0xXRTJZamd4TURNeFpUYzJZeUlzSW5SbGMzUnBaQ0k2SW1GMGRHMXZZbWxzYVhSNUlpd2lkWE5sY21sa0lqb2lOREV3TmpJd05qazRNRGMwTWpRd09EQXpOQ0lzSW1ScGJXVnVjMmx2Ym5kcFpIUm9Jam9pTnpJNElpd2laR2x0Wlc1emFXOXVhR1ZwWjJoMElqb2lPVEFpTENKamJHbGphM1Z5YkNJNklpSXNJbU55WldGMGFYWmxhV1FpT2lJek9UZ3dNVE00T0NJc0ltRmtkbVp5WlhFaU9pSXdJaXdpWVhWamRHbHZibWxrSWpvaU1qRXpPVGMxTURRNE5ERTFPRFF6TkRZd09DSXNJbkJ5YVdObGNHRnBaQ0k2SWpNdU1ETXlPVFkzSWl3aVlXNWtjbTlwWkdsa0lqb2lJaXdpWVhCd2JHVnBaQ0k2SWlJc0luQjFZbXhwYzJobGNtbGtJam9pTVRjNU1Ea2lMQ0poWjJVaU9pSXdJaXdpY21WbWRYSnNJam9pYUhSMGNEb3ZMM2QzZHk1bFltRjVMbU52YlM4aUxDSnpaV3hzWlhKdFpXMXBaQ0k2SWpFNE1TSXNJbk5sYzNObWNtVnhJam9pTFRFaUxDSnphWFJsYVdRaU9pSTFOVFF4T0NJc0lucHBjR052WkdVaU9pSXlOelExTlNJc0ltTnBkSGtpT2lKSGNtVmxibk5pYjNKdklpd2lZMjkxYm5SeWVTSTZJbFZUSWl3aWRYTmxjbWx3SWpvaU1UVXlMakV6TGpJME9TNHdJaXdpWW1sa2NISnBZMlVpT2lJekxqUXhOVGcxTnlJc0ltTnlaV0YwYVhabGMybDZaU0k2SWpjeU9IZzVNQ0lzSW1SdGNISmxjM1J5YVdOMGFXOXVZWEJ3YkdsbGN5STZJblJ5ZFdVaUxDSnZjSFJ2ZFhSeVpYTjBjbWxqZEdsdmJtRndjR3hwWlhNaU9pSm1ZV3h6WlNJc0luSmxjWFZsYzNScFpDSTZJbU0wTlRSaU1qY3pMVFpoWkdRdE5HSm1OQzA0TmpnNUxUUmpOalU0WTJKa01EUTBNaUlzSW1OaGJYQmhhV2R1YVdRaU9pSXhNREkzTWpnMk1pSjlmWDAiLCJ0diI6ImpzLTIuNC4zIiwidG5hIjoiY28iLCJhaWQiOiJhZGltcHJlc3Npb25fYXR0bW9iaWxpdHkiLCJwIjoid2ViIiwidHoiOiJBbWVyaWNhL05ld19Zb3JrIiwibGFuZyI6ImVuLVVTIiwiY3MiOiJ3aW5kb3dzLTEyNTIiLCJmX3BkZiI6IjEiLCJmX3F0IjoiMCIsImZfcmVhbHAiOiIwIiwiZl93bWEiOiIwIiwiZl9kaXIiOiIwIiwiZl9mbGEiOiIxIiwiZl9qYXZhIjoiMCIsImZfZ2VhcnMiOiIwIiwiZl9hZyI6IjEiLCJyZXMiOiIxOTIweDEwODAiLCJjZCI6IjI0IiwiY29va2llIjoiMSIsImVpZCI6IjFhZDUyNjAwLTE0ZTQtNDI2Zi05NDkyLWY0ZWUzMDE3N2Y3ZSIsImR0bSI6IjE0NTgyMzczNjA4ODUiLCJ2cCI6IjcyOHg5MCIsImRzIjoiNzI4eDkxIiwidmlkIjoiMSIsImR1aWQiOiJkMzhiMmY2ODFiNWE3NjQ0IiwiZnAiOiIyOTgyNzIxMDYzIiwicmVmciI6Imh0dHA6Ly93d3cucGxhY2Vob2xkZXIuY29tIiwidXJsIjoiaHR0cDovL3d3dy5wbGFjZWhvbGRlci5jb20ifV19")
        }
      }
      .run
      .finish
  }


}
