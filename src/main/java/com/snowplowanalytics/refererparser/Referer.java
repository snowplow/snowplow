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
 * Referer data class
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public class Referer {
  public final Medium medium;
  public final String source, term;

  public Referer(Medium medium, String source, String term) {
    this.medium = medium;
    this.source = source;
    this.term = term;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Referer)) return false;

    Referer r = (Referer) other;
    return ((this.medium != null && this.medium.equals(r.medium)) || this.medium == r.medium) &&
           ((this.source != null && this.source.equals(r.source)) || this.source == r.source) &&
           ((this.term != null && this.term.equals(r.term)) || this.term == r.term);
  }

  @Override
  public int hashCode() {
    int h = medium == null ? 0 : medium.hashCode();
    h += source == null ? 0 : source.hashCode();
    h += term == null ? 0 : term.hashCode();
    return h;
  }

  @Override
  public String toString() {
    return String.format("{medium: %s, source: %s, term: %s}",
                         medium, source, term);
  }
}