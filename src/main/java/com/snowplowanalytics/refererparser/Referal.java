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
 * Referal data class
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public class Referal {
  public final Referer referer;
  public final Search search;

  public Referal(Referer referer, Search search) {
    this.referer = referer;
    this.search = search;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Referal)) return false;

    Referal r = (Referal) other;
    return ((this.referer != null && this.referer.equals(r.referer)) || this.referer == r.referer) &&
           ((this.search != null && this.search.equals(r.search)) || this.search == r.search);
  }

  @Override
  public int hashCode() {
    int h = referer == null ? 0 : referer.hashCode();
    h += search == null ? 0 : search.hashCode();
    return h;
  }

  @Override
  public String toString() {
    return String.format("{referer: %s, search: %s}",
                         referer, search);
  }
}