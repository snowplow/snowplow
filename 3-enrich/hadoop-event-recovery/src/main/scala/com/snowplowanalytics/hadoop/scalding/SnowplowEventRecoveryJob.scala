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

// Scalding
import com.twitter.scalding.{JsonLine => StandardJsonLine, _}

// Cascading
import cascading.tuple.Fields
import cascading.tap.SinkMode

// Commons
import org.apache.commons.codec.binary.Base64
import java.nio.charset.StandardCharsets.UTF_8

object JsonLine {
  def apply(p: String, fields: Fields = Fields.ALL) = new JsonLine(p, fields)
}
class JsonLine(p: String, fields: Fields) extends StandardJsonLine(p, fields, SinkMode.REPLACE) {
  // We want to test the actual tranformation here.
  override val transformInTest = true
}

/**
 * Scalding job to make bad rows ready for reprocessing
 *
 * @param args Arguments to the job
 */
class SnowplowEventRecoveryJob(args : Args) extends Job(args) {

  lazy val preprocessor = args("inputFormat") match {
    case "bad" => BadRowReprocessor
    case "raw" => RawLinePreprocessor
  }

  lazy val processor = new JsProcessor(new String(Base64.decodeBase64(args("script")), UTF_8))

  val inputPatterns = args("input").split(",")

  MultipleTextLineFiles(inputPatterns: _*).read
    .flatMapTo('line -> 'altered) { line: String =>
      val (inputTsv, errorStrings) = preprocessor.preprocess(line)
      processor.process(inputTsv, errorStrings)
    }
    .write(Tsv(args("output")))
}
