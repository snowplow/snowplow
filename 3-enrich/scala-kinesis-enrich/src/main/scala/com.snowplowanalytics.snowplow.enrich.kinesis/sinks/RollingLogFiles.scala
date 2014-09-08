/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis
package sinks

// Logging
import java.util.Iterator
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.rolling.{FixedWindowRollingPolicy, RollingFileAppender, SizeBasedTriggeringPolicy}
import ch.qos.logback.core.util.StatusPrinter

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift._

/**
 * RollingLogFiles Sink for Scala enrichment
 */
class RollingLogFilesSink(config: KinesisEnrichConfig) extends ISink {
  val loggerContext: LoggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]

  val rfAppender = new RollingFileAppender[ILoggingEvent]()
  rfAppender.setContext(loggerContext)
  rfAppender.setFile(config.logFile)
  val rollingPolicy = new FixedWindowRollingPolicy()
  rollingPolicy.setContext(loggerContext)
  // rolling policies need to know their parent
  // it's one of the rare cases, where a sub-component knows about its parent
  rollingPolicy.setParent(rfAppender)
  rollingPolicy.setFileNamePattern("/tmp/sp.%i.log.gz")
  rollingPolicy.start()

  val triggeringPolicy = new SizeBasedTriggeringPolicy[ILoggingEvent]()
  triggeringPolicy.setContext(loggerContext)
  triggeringPolicy.setMaxFileSize(config.logMaxSize)
  triggeringPolicy.start()

  val encoder = new PatternLayoutEncoder();
  encoder.setContext(loggerContext);
  encoder.setPattern("%msg%n");
  encoder.start();

  rfAppender.setEncoder(encoder)
  rfAppender.setRollingPolicy(rollingPolicy)
  rfAppender.setTriggeringPolicy(triggeringPolicy)
  rfAppender.start()

  // attach the rolling file appender to the logger of your choice
  val log = loggerContext.getLogger(getClass())
  // detach from root logger
  log.setAdditive(false)
  log.addAppender(rfAppender)

  // OPTIONAL: print logback internal status messages
  StatusPrinter.print(loggerContext)

  /**
   * Side-effecting function to store the CanonicalOutput
   * to the given output stream.
   *
   * CanonicalOutput takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   */
  def storeCanonicalOutput(output: String, key: String) {
    log.debug(output)
  }
}
