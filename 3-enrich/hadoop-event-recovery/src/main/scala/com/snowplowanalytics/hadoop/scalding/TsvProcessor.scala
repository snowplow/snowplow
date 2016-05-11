/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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

/**
 * Object to either discard or mutate a bad row containing a TSV raw event
 */
trait TsvProcessor {
  
  /**
   * Decide whether to try to fix up a given bad row, then act accordingly
   *
   * @param inputTsv The tab-separated raw event in the Cloudfront Access Log format
   * @param errors An array of errors describing why the inputTsv is invalid
   * @return Some(mutatedInputTsv), or None if this bad row should be ignored
   */
  def process(inputTsv: String, errors: Seq[String]): Option[String]
}
