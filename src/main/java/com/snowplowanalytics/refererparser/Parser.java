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

// Apache URLEncodedUtils
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

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

  public Referal parse(String refererUri) {
    if (refererUri == null || refererUri == "") return null;
    final URI uri = new URI(refererUri);
    return parse(uri);
  }

  public Referal parse(URI refererUri) {

    // null unless we have a valid http: or https: URI
    if (refererUri == null) return null;
    final String scheme = refererUri.getScheme();
    if (scheme != "http" && scheme != "https") return null;

    // Check if domain+path matches (e.g. google.co.uk/products)
    Map<String,List>referer = referers.get(refererUri.getHost() + refererUri.getPath());
    if (referer == null) {
      referer = referers.get(refererUri.getHost() + refererUri.getPath());
    }

    // Create our referer as necessary
    if (referer == null) {
      return new Referal(new Referer("Other"), null); // Other referer, no search we can extract
    } else {
      final Referer refr = new Referer(referer.get("name"));
      final Search search = extractSearch(refererUri, possibleParameters);
      return new Referal(refr, search);
    }
  }

  private Search extractSearch(URI uri, List<String> possibleParameters) {

    List<NameValuePair> params = URLEncodedUtils.parse(uri, "UTF-8");

    for (NameValuePair pair : params) {
      final String name = pair.getName();
      final String value = pair.getValue();

      if possibleParameters.contains(name) {
        return new Search(value, name);
      }
    }
    return null;
  }

  private Map<String,Map> loadReferers(InputStream referersYaml) {

    Yaml yaml = new Yaml(new SafeConstructor());
    Map<String,Map> rawReferers = (Map<String,Map>) yaml.load(referersYaml);

    // Process each element of the map
    Map<String,Map> referers = new Map<String,Map>();
    for (Map.Entry<String, Map> referer : rawReferers.entrySet()) {

      String refererName = referer.getKey();
      String refererMap = referer.getValue();

      // Validate
      List<String> parameters = refererMap.get("parameters");
      if (parameters == null) {
        throw new CorruptReferersYamlException("No parameters found for referer '" + refererName + "'");
      }
      List<String> domains = refererMap.get("domains");
      if (domains == null) { 
        throw new CorruptReferersYamlException("No domains found for referer '" + refererName + "'");
      }

      // Our hash needs referer domain as the
      // key, so let's expand
      for (String domain : domains) {
        Map<String,Map> domainMap = new Map<String,Map>();
        domainMap.put("name", refererName);
        domainMap.put("parameters", parameters);

        referers.add(domain, domainMap);
      }
    }

    return referers;
  }
}