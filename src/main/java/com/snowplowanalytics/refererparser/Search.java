/**
 * Copyright 2012 SnowPlow Analytics Ltd
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
 * Search data class
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public class Search {
  public final String term, parameter;

  public Search(String term, String parameter) {
    this.term = term;
    this.parameter = parameter;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Search)) return false;

    Search s = (Search) other;
    return ((this.term != null && this.term.equals(s.term)) || this.term == s.term) &&
           ((this.parameter != null && this.parameter.equals(s.parameter)) || this.parameter == s.parameter);
  }

  @Override
  public int hashCode() {
    int h = term == null ? 0 : term.hashCode();
    h += parameter == null ? 0 : parameter.hashCode();
    return h;
  }

  @Override
  public String toString() {
    return String.format("{term: %s, parameter: %s}",
                         term == null ? null : '"' + term + '"',
                         parameter == null ? null : '"' + parameter + '"');
  }
}