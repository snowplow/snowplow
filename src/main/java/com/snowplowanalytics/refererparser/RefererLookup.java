/**
 * Copyright 2012-2018 Snowplow Analytics Ltd
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

import java.util.List;

/**
* Holds the structure of each referer
* in our lookup Map.
*/
public class RefererLookup {
  public Medium medium;
  public String source;
  public List<String> parameters;

  public RefererLookup(Medium medium, String source, List<String> parameters) {
    this.medium = medium;
    this.source = source;
    this.parameters = parameters;
  }
}
