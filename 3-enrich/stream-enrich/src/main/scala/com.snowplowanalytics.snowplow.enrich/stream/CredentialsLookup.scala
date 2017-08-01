 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich.stream

// Amazon
import com.amazonaws.auth._

/**
 * Gets AWS credentials based on configuration YAML
 */
object CredentialsLookup {

  /**
   * Returns AWS credentials based on access key and secret key
   *
   * @param a Access key
   * @param s Secret key
   * @return An AWSCredentialsProvider
   */
  def getCredentialsProvider(a: String, s: String): AWSCredentialsProvider = {
    if (isDefault(a) && isDefault(s)) {
      new DefaultAWSCredentialsProviderChain()
    } else if (isDefault(a) || isDefault(s)) {
      throw new RuntimeException(
        "access-key and secret-key must both be set to 'default', or neither"
      )
    } else if (isIam(a) && isIam(s)) {
      new InstanceProfileCredentialsProvider()
    } else if (isIam(a) || isIam(s)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'iam', or neither")
    } else if (isEnv(a) && isEnv(s)) {
      new EnvironmentVariableCredentialsProvider()
    } else if (isEnv(a) || isEnv(s)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'env', or neither")
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(a, s)
      )
    }
  }

  /**
   * Is the access/secret key set to the special value "default" i.e. use
   * the standard provider chain for credentials.
   *
   * @param key The key to check
   * @return true if key is default, false otherwise
   */
  private def isDefault(key: String): Boolean = (key == "default")

  /**
   * Is the access/secret key set to the special value "iam" i.e. use
   * the IAM role to get credentials.
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isIam(key: String): Boolean = (key == "iam")

  /**
   * Is the access/secret key set to the special value "env" i.e. get
   * the credentials from environment variables
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isEnv(key: String): Boolean = (key == "env")

  // Wrap BasicAWSCredential objects.
  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
