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

import scala.util.parsing.json._

trait Preprocessor {
  def preprocess(line: String): (String, Seq[String])
}

object RawLinePreprocessor extends Preprocessor {
  def preprocess(line: String): (String, Seq[String]) = (line, Nil)
}

object BadRowReprocessor extends Preprocessor {
  def preprocess(line: String): (String, Seq[String]) = {
    val parsedJson = JSON.parseFull(line).get.asInstanceOf[Map[String, Object]]
    val inputTsv = parsedJson("line").asInstanceOf[String]
    val errs = parsedJson("errors").asInstanceOf[Seq[Object]]

    // We need to determine whether these are old-style errors of the form ["errorString1", ...]
    // or new-style ones of the form [{"level": "..", "message": "errorString1"}]
    val errorStrings = if (errs.isEmpty) {
      Nil
    } else {
      errs(0) match {
        case s: String => errs.asInstanceOf[Seq[String]]
        case _ => errs.asInstanceOf[Seq[Map[String, Object]]].map(_("message").asInstanceOf[String])
      }
    }
    (inputTsv, errorStrings)
  }
}
