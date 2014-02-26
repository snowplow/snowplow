package com.snowplowanalytics.refererparser;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;
import org.json.JSONException;
import org.json.JSONTokener;
import org.json.JSONObject;
import org.json.JSONArray;

public class ParserTest {

  private Parser parser;

  @Before
  public void createParser() throws CorruptYamlException, IOException {
    parser = new Parser();
  }

  @Test
  public void refererTests() throws MalformedURLException, JSONException {
    JSONTokener tok = new JSONTokener( ParserTest.class.getResourceAsStream("/referer-tests.json"));
    JSONArray root = new JSONArray(tok);
    
    for (int i = 0; i < root.length(); i++) {
      JSONObject entry = (JSONObject) root.get(i);

      Referer actual = parser.parse(new URL(entry.getString("uri")), "www.snowplowanalytics.com");
      assertEquals(entry.getString("spec") + " medium", entry.getString("medium"), actual.medium.toString());
      assertEquals(entry.getString("spec") + " source", entry.get("source"), actual.source);
      assertEquals(entry.getString("spec") + " term", entry.get("term"), actual.term);
    }
  }
  
  @Test
  public void basicTests() throws URISyntaxException, MalformedURLException, JSONException{
    //Test every signature.
    String u = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari";
    String ref = "www.example.com";
    String expected = "{medium: search, source: Google, term: gateway oracle cards denise linn}";
    
    //URI, URI
    Referer result = parser.parse(new URI(u), new URI("http://"+ref));
    assertEquals("URI, URI", expected, result.toString());
    
    //String, URI
    result = parser.parse(u, new URI("http://"+ref));
    assertEquals("String, URI", expected, result.toString());
    
    //String, String
    result = parser.parse(u, ref);
    assertEquals("String, String", expected, result.toString());
    
    //URI, String
    result = parser.parse(new URI(u), ref);
    assertEquals("URI, String", expected, result.toString());
    
    //URL, String
    result = parser.parse(new URL(u), ref);
    assertEquals("URL, String", expected, result.toString());
  }
}
