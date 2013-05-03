/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowplowanalytics.refererparser;

/**
 * Enum for the types of referer
 * that we can detect - "medium"
 * in Google's language.
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public enum Medium {
    UNKNOWN,
    INTERNAL,
    SEARCH,
    SOCIAL,
    EMAIL;

  static public Medium fromString(String medium) {
    return Medium.valueOf(medium.toUpperCase());
  }

  @Override
  public String toString() {
    return super.toString().toLowerCase();
  }
}