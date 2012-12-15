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

// Java
import java.net.URI;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

// SnakeYAML
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Java implementation of <a href="https://github.com/snowplow/referer-parser">Referer Parser</a>
 *
 * @author Alex Dean (@alexatkeplar) <support at snowplowanalytics com>
 */
public class Parser {

  private static final String REFERERS_YAML_PATH = "/referer_parser/search_engines.yaml";
  private Map<String,Map> referers;

  public Parser() throws IOException {
    this(Parser.class.getResourceAsStream(REFERERS_YAML_PATH));
  }

  public Parser(InputStream referersYaml) {
    referers = loadReferers(referersYaml);
  }

  public Referer parse(String refererUri) {
    static final URI uri = new URI(refererUri);
    return parse(uri);
  }

  public Referer parse(URI refererUri) {

    // First check we have an http: or https: URI
    String scheme = refererUri.getScheme();
    if (scheme != "http" && scheme != "https") {
      throw new IllegalArgumentException('"' + scheme + '" is not an http(s) protocol URI');
    }

    // Check if domain+path matches (e.g. google.co.uk/products)
    Map<String,List>referer = referers.get(refererUri.getHost() + refererUri.getPath());
    if (referer == null) {
      referer = referers.get(refererUri.getHost() + refererUri.getPath());
    } // May still be null.

    // Create our referer as necessary
    if (referer == null) {
      return new Referer(null, false, null, null); // Referer is not known
    } else {
      String name = referer.get("name");
      String searchTerm = null; // How to do a tuple in Java?
      String searchParameter = null; // Ditto
      return new Referer(name, true, searchParameter, searchTerm)
    }
  }

  private Map<String,Map> loadReferers(InputStream referersYaml) {

    Yaml yaml = new Yaml(new SafeConstructor());
    Map<String,Map> rawReferers = (Map<String,Map>) yaml.load(referersYaml);

    // Process each element of the map
    Map<String,Map> referers = (Map<String,Map>);
    for (String referer : rawReferers.getKeys()) { // Pseudo-code
      Map<String,List> refererMap = rawRefers.get(referer); // Can I do this in one line?

      // Validate
      List<String> parameters = refererMap.get("parameters");
      if (parameters == null) {
        throw new CorruptReferersYamlException('No parameters found for referer "' + referer + '"');
      }
      List<String> domains = refererMap.get("domains");
      if (domains == null) { 
        throw new CorruptReferersYamlException('No domains found for referer "' + referer + '"');
      }

      // Our hash needs referer domain as the
      // key, so let's expand
      for (String domain : domains) {
        Map<String,Map> domainMap = (Map<String,Map>);
        domainMap.put("name", referer); // Pseudo-code
        domainMap.put("parameters", parameters);

        referers.add(domain, domainMap);
      }
    }

    return referers;
  }
}