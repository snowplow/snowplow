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
package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

// Java
import java.io.FileNotFoundException

// Config
import com.typesafe.config.Config
import com.typesafe.config.ConfigException

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Test SnowplowBigqueryEmitter, along with config files.
 */
class SnowplowBigqueryEmitterSpec extends Specification with ValidationMatchers {
 
  val testConfigFileGood = "src/test/resources/application.conf.test"
  val testConfigFileBad = "src/test/resources/application.conf.badtest"
  val testConfigFileMissing = "src/test/resources/application.conf.missingtest"

  "getConfigFromFile" should {
    "for non-existant file: throw an exception" in {
      SnowplowBigqueryEmitter.getConfigFromFile(testConfigFileMissing) must throwA[FileNotFoundException]
    }

    val badReturned = SnowplowBigqueryEmitter.getConfigFromFile( testConfigFileBad)

    """for a file with missing 'project-name' attribute:
      -- throw a ConfigException.Missing exception""" in {
      badReturned.getString("connector.bigquery.project-number") must throwA[ConfigException.Missing]
    }

    val goodReturned = SnowplowBigqueryEmitter.getConfigFromFile( testConfigFileGood )

    "for a good file: return a Config object" in {
      goodReturned must haveInterface[Config]
    }
    """for a good file: the object should have the following attributes
      -- project-number:""" in {
      goodReturned.getString("connector.bigquery.project-number") must beEqualTo("123456789")
    }
    " -- dataset-name:" in {
      goodReturned.getString("connector.bigquery.dataset-name") must beEqualTo("datasetName")
    }
    " -- table-name:" in {
      goodReturned.getString("connector.bigquery.table-name") must beEqualTo("tableName")
    }
  }
}

