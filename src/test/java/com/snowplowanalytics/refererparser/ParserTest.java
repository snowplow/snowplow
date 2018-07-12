package com.snowplowanalytics.refererparser;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;

public class ParserTest {

  private Parser parser;

  @Before
  public void createParser() throws CorruptJsonException, IOException {
    parser = new Parser();
  }

  @Test
  public void basicTests() throws URISyntaxException, MalformedURLException {
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
