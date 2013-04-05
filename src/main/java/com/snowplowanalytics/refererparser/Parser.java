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
import java.net.URISyntaxException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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

  private static final String REFERERS_YAML_PATH = "/referers.yml";
  private Map<String,RefererLookup> referers;

  private static class RefererLookup {
    public Medium medium;
    public String source;
    public List<String> parameters;

    public RefererLookup(Medium medium, String source, List<String> parameters) {
      this.medium = medium;
      this.source = source;
      this.parameters = parameters;
    }
  }

  public Parser() throws IOException, CorruptYamlException {
    this(Parser.class.getResourceAsStream(REFERERS_YAML_PATH));
  }

  public Parser(InputStream referersYaml) throws CorruptYamlException {
    referers = loadReferers(referersYaml);
  }

  public Referer parse(URI refererUri, URI pageUri) {
    return parse(refererUri, pageUri.getHost());
  }

  public Referer parse(String refererUri, URI pageUri) throws URISyntaxException {
    return parse(refererUri, pageUri.getHost());
  }

  public Referer parse(String refererUri, String pageHost) throws URISyntaxException {
    if (refererUri == null || refererUri == "") return null;
    final URI uri = new URI(refererUri);
    return parse(uri, pageHost);
  }

  public Referer parse(URI refererUri, String pageHost) {

    // null unless we have a valid http: or https: URI
    if (refererUri == null) return null;
    final String scheme = refererUri.getScheme();
    if (!scheme.equals("http") && !scheme.equals("https")) return null;

    // Internal link if hosts match exactly
    // TODO: would also be nice to:
    // 1. Support a list of other hosts which count as internal
    // 2. Have an algo for stripping subdomains before checking match
    if (refererUri.getHost() == pageHost) return new Referer(Medium.INTERNAL, null, null);

    // Try to lookup our referer
    RefererLookup referer = lookupReferer(refererUri.getHost(), refererUri.getPath());
    if (referer == null) {
      return new Referer(Medium.UNKNOWN, null, null); // Unknown referer, nothing more to do
    } else {
      final String term = (referer.medium == Medium.SEARCH) ? extractSearchTerm(refererUri, referer.parameters) : null;
      return new Referer(referer.medium, referer.source, term);
    }
  }

  /**
   * Recursive function to lookup a host (or partial host)
   * in our referers map.
   *
   * First check the host, then the host+path
   * If not found, remove one subdomain-level off the front
   * of the host and try again.
   *
   * @param pageHost The host of the current page
   * @param pagePath The path to the current page
   *
   * @return a RefererLookup object populated with the given
   *         referer, or null if not found
   */
  private RefererLookup lookupReferer(String refererHost, String refererPath) {

    // Check if domain+path matches (e.g. google.co.uk/products)    
    RefererLookup referer = referers.get(refererHost + refererPath);
    if (referer == null) {
      referer = referers.get(refererHost); // Try just the domain (e.g. google.com)
      if (referer == null) {
        int idx = refererHost.indexOf('.');
        if (idx == -1) {
          return null; // No "."? Let's quit.
        } else {
          lookupReferer(refererHost.substring(idx + 1), refererPath); // Recurse
        }
      } else {
        return referer;
      }
    }
    return referer;
  }

  private String extractSearchTerm(URI uri, List<String> possibleParameters) {

    List<NameValuePair> params = URLEncodedUtils.parse(uri, "UTF-8");

    for (NameValuePair pair : params) {
      final String name = pair.getName();
      final String value = pair.getValue();

      if (possibleParameters.contains(name)) {
        return value;
      }
    }
    return null;
  }

  /**
   * Builds the map of hosts to referers from the
   * input YAML file.
   *
   * @param referersYaml An InputStream containing the
   *                     referers database in YAML format.
   *
   * @return a Map where the key is the hostname of each
   *         referer and the value (RefererLookup)
   *         contains all known info about this referer
   */
  private Map<String,RefererLookup> loadReferers(InputStream referersYaml) throws CorruptYamlException {

    Yaml yaml = new Yaml(new SafeConstructor());
    Map<String,Map<String,Map>> rawReferers = (Map<String,Map<String,Map>>) yaml.load(referersYaml);

    // This will store all of our referers
    Map<String,RefererLookup> referers = new HashMap<String,RefererLookup>();

    // Outer loop is all referers under a given medium
    for (Map.Entry<String,Map<String,Map>> mediumReferers : rawReferers.entrySet()) {

      Medium medium = Medium.fromString(mediumReferers.getKey());

      // Inner loop is individual referers
      for (Map.Entry<String,Map> referer : mediumReferers.getValue().entrySet()) {

        String sourceName = referer.getKey();
        Map<String,List<String>> refererMap = referer.getValue();

        // Validate
        List<String> parameters = refererMap.get("parameters");
        if (medium == Medium.SEARCH) {
          if (parameters == null) {
            throw new CorruptYamlException("No parameters found for search referer '" + sourceName + "'");
          }
        } else {
          if (parameters != null) {
            throw new CorruptYamlException("Parameters not supported for non-search referer '" + sourceName + "'");
          }
        }
        List<String> domains = refererMap.get("domains");
        if (domains == null) { 
          throw new CorruptYamlException("No domains found for referer '" + sourceName + "'");
        }

        // Our hash needs referer domain as the
        // key, so let's expand
        for (String domain : domains) {
          if (referers.containsValue(domain)) {
            throw new CorruptYamlException("Duplicate of domain '" + domain + "' found");
          }
          referers.put(domain, new RefererLookup(medium, sourceName, parameters));
        }
      }
    }

    return referers;
  }
}