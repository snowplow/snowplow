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
 * Referer data class
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public class Referer {
  public final String name, searchTerm, searchParameter;
  public final boolean known;

  public Referer(String name, boolean known, String searchParameter, String searchTerm) {
    this.name = name;
    this.known = known;
    this.searchParameter = searchParameter;
    this.searchTerm = searchTerm;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (this.known == false) return false;
    if (!(other instanceof OS)) return false;

    Referer r = (Referer) other;
    return ((this.name != null && this.name.equals(r.name)) || this.name == r.name) &&
           ((this.searchParameter != null && this.searchParameter.equals(r.searchParameter)) || this.searchParameter == r.searchParameter) &&
           ((this.searchTerm != null && this.searchTerm.equals(r.searchTerm)) || this.searchTerm == r.searchTerm);
  }

  @Override
  public int hashCode() {
    int h = name == null ? 0 : name.hashCode();
    h += known == null ? 0 : known.hashCode();
    h += searchParameter == null ? 0 : searchParameter.hashCode();
    h += searchTerm == null ? 0 : searchTerm.hashCode();
    return h;
  }

  @Override
  public String toString() {
    return String.format("{name: %s, known: %s, search_parameter: %s, search_term: %s}",
                         name == null ? null : '"' + name + '"',
                         known == null ? null : '"' + known + '"',
                         searchParameter == null ? null : '"' + searchParameter + '"',
                         searchTerm == null ? null : '"' + searchTerm + '"');
  }
}