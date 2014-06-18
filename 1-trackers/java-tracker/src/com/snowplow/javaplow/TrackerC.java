/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplow.javaplow;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 *  <p>TrackerC is the implementing class for the Tracker interface.</p>
 *  <p>It can be used to track many different types of events. The current version
 *   of the tracker is 0.1.0 meaning it is functional but likely contains some bugs.
 *   If you find any please contact me at kaagleason@gmail.com or contact snowplow
 *   via their website.</p>
 *
 *  <p><b>To use:</b></p>
 *  <ul>
 *      <li>Import the com.snowplow.javaplow package</li>
 *      <li>You must first declare a payload and a tracker</li>
 *      <li>Tracker will instantiate with a good-for-tracking payload</li>
 *      <li>(recommended) Build the payload identity with TrackerC set functions (e.g setUser(String user))</li>
 *      <li>(optional) Add custom payload to Tracker with setPayload(). <br/>Payload will configure itself further and prepare for request.</li>
 *      <li>Call a specific tracking event. Payload will be automatically configured further</li>
 *      <li>Get request is sent to server</li>
 *  </ul>
 *
 *  <p><b>There are two static functions in the TrackerC class: "debug" and "track"</b></p>
 *  <ul>
 *    <li>To see the output you are sending to the web as it is sending, put `TrackerC.debug = true` in your code.</li>
 *    <li>If you would like to test what the output would look like in your usage scenario without<br/>
 *      sending logs to your S3 bucket, put `TrackerC.track = false` in your code.</li>
 *    <li>Default values are `TrackerC.debug = false` and `TrackerC.track = true`</li>
 *  </ul>
 *  @version 0.1.0
 *  @author Kevin Gleason
 */

public class TrackerC implements Tracker {
    //Static Class variables
    private static final String VERSION = Version.VERSION;
    private static final String DEFAULT_PLATFORM = "pc";
    public static final String DEFAULT_VENDOR = "com.saggezza";

    //DEBUG
    public static boolean debug = false;
    public static boolean track = true;

    //Instance Variables
    private PayloadMap payload = new PayloadMapC();
    private PlowContractor<String> stringContractor = new PlowContractor<String>();
    private PlowContractor<Integer> integerContractor = new PlowContractor<Integer>();
    private PlowContractor<Map<String, Object>> dictionaryContractor = new PlowContractor<Map<String, Object>>();
    private String collector_uri,
                   namespace,
                   app_id,
                   context_vendor;
    private boolean base64_encode,
                    contracts;


    //Base Constructor
    public TrackerC(String collector_uri, String namespace) {
        this.collector_uri = (collector_uri);
        this.namespace = namespace;
        this.app_id = this.context_vendor = "";
        this.base64_encode = this.contracts = true;
        this.setPayload(new PayloadMapC());
    }

    //Constructor with all arguments
    public TrackerC(String collector_uri, String namespace, String app_id,
                    String context_vendor, boolean base64_encode, boolean contracts) {
        this.collector_uri = (collector_uri);
        this.namespace = namespace;
        this.app_id = app_id;
        this.context_vendor = context_vendor;
        this.base64_encode = base64_encode;
        this.contracts = contracts;
        this.setPayload(new PayloadMapC());
    }

    /**
     * {@inheritDoc}
     * @throws URISyntaxException
     * @throws IOException
     */
    public void track() throws URISyntaxException, IOException{
        URI uri = buildURI("https", collector_uri, "/i");
        this.payload = this.payload.setTimestamp();
        HttpGet httpGet = makeHttpGet(uri);
        if (debug) {
            System.out.println("Payload:\n" + this.payload.toString());
            System.out.println("URI: " + uri);
            System.out.println("Making HttpGet...");
        }
        if (track)
            makeRequest(httpGet);
        this.clearPayload();
   }

    /**
     * {@inheritDoc}
     * @param page_url The url of the page where the tracking call lies.
     * @param page_title The title of the page where the tracking call lies. (optional)
     * @param referrer The one who referred you to the page (optional)
     * @param context Additional JSON context for the tracking call (optional)
     * @throws URISyntaxException
     * @throws JSONException
     * @throws IOException
     */
    public void track_page_view(String page_url, String page_title, String referrer, String context)
            throws URISyntaxException, JSONException, IOException{
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, page_url);
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_page_view_config(page_url, page_title, referrer, jsonContext);
        }
        else {
            this.payload = this.payload.track_page_view_config(page_url, page_title, referrer, null);
        }
        this.track();
    }

    /**
     * {@inheritDoc}
     * @param category The category of the structured event.
     * @param action The action that is being tracked. (optional)
     * @param label A label for the tracking event. (optional)
     * @param property The property of the structured event being tracked. (optional)
     * @param value The value associated with the property being tracked.
     * @param vendor The vendor the the property being tracked. (optional)
     * @param context Additional JSON context for the tracking call (optional)
     * @throws JSONException
     * @throws URISyntaxException
     * @throws IOException
     */
    public void track_struct_event(String category, String action, String label, String property,
                                   int value, String vendor, String context)
            throws JSONException, URISyntaxException, IOException {
        String valueStr = String.valueOf(value);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, category);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, valueStr);
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_struct_event_config(category, action, label, property, valueStr,
                    jsonContext);
        } else {
            this.payload = this.payload.track_struct_event_config(category, action, label, property, valueStr,
                    null);
        }
        this.track();
    }

    /**
     * {@inheritDoc}
     * @param eventVendor The vendor the the event information.
     * @param eventName A name for the unstructured event being tracked.
     * @param dictInfo The unstructured information being tracked in dictionary form.
     * @param context Additional JSON context for the tracking call (optional)
     * @throws JSONException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void track_unstruct_event(String eventVendor, String eventName, Map<String, Object> dictInfo, String context)
            throws JSONException, IOException, URISyntaxException{
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, eventVendor);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, eventName);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_dict, dictInfo.toString());
        JSONObject jsonDict = mapToJSON(dictInfo); //Make compatible for Map<String, Object>
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_unstruct_event_config(eventVendor, eventName, jsonDict, jsonContext);
        } else {
            this.payload = this.payload.track_unstruct_event_config(eventVendor, eventName, jsonDict, null);
        }
        this.track();
    }

    /**
     * {@inheritDoc}
     * @param eventVendor The vendor the the event information.
     * @param eventName A name for the unstructured event being tracked.
     * @param dictInfo The unstructured information being tracked in dictionary form.
     * @param context Additional JSON context for the tracking call (optional)
     * @throws JSONException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void track_unstruct_event(String eventVendor, String eventName, String dictInfo, String context)
            throws JSONException, IOException, URISyntaxException{
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, eventVendor);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, eventName);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_dict, dictInfo.toString());
        JSONObject jsonDict = stringToJSON(dictInfo); //Make compatible for Map<String, Object>
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_unstruct_event_config(eventVendor, eventName, jsonDict, jsonContext);
        } else {
            this.payload = this.payload.track_unstruct_event_config(eventVendor, eventName, jsonDict, null);
        }
        this.track();
    }



    /**
     * {@inheritDoc}
     * @param name The name of the screen view being tracked
     * @param id The ID of the screen view being tracked.
     * @param context Additional JSON context for the tracking call (optional)
     * @throws JSONException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void track_screen_view(String name, String id, String context)
            throws JSONException, IOException, URISyntaxException{
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, name);
        Map<String, Object> screenViewProperties = new LinkedHashMap<String, Object>();
        screenViewProperties.put("Name", name); // or String screenVie... = "{'name': '"+ name + "'}"
        if (id != null)
            this.payload.add("id", id);
        this.track_unstruct_event(DEFAULT_VENDOR, "screen_view", screenViewProperties, context);
    }

    /**
     * {@inheritDoc}
     * @param order_id ID of the item.
     * @param sku SKU value of the item.
     * @param price Prive of the item.
     * @param quantity Quantity of the item.
     * @param name Name of the item.
     * @param category Category of the item.
     * @param currency Currency used for the purchase.
     * @param context Additional JSON context for the tracking call (optional)
     * @param transaction_id Transaction ID, if left blank new value will be generated.
     * @throws JSONException
     * @throws URISyntaxException
     * @throws IOException
     */
    public void track_ecommerce_transaction_item(String order_id, String sku, Double price, Integer quantity,
            String name, String category, String currency, String context, String transaction_id)
            throws JSONException, URISyntaxException, IOException {
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, order_id);
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, sku);
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_ecommerce_transaction_item_config(order_id, sku, doubleCheck(price),
                    integerCheck(quantity), stringCheck(name), stringCheck(category), stringCheck(currency), jsonContext, null);
        } else {
            this.payload = this.payload.track_ecommerce_transaction_item_config(order_id, sku, doubleCheck(price),
                    integerCheck(quantity), stringCheck(name), stringCheck(category), stringCheck(currency), null, null);
        }
        this.track();
    }

    /**
     * {@inheritDoc}
     * @param order_id The transaction ID, will be generated if left null
     * @param total_value The total value of the transaction.
     * @param affiliation Affiliations to the transaction (optional)
     * @param tax_value Tax value of the transaction (optional)
     * @param shipping Shipping costs of the transaction (optional)
     * @param city The customers city.
     * @param state The customers state.
     * @param country The customers country.
     * @param currency The currency used for the purchase
     * @param items A list containing a Map of Strings. Each item must have order ID, sku, price, and quantity.
     * @param context Additional JSON context for the tracking call (optional)
     * @throws JSONException
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws URISyntaxException
     */
    public void track_ecommerce_transaction(String order_id, Double total_value, String affiliation, Double tax_value,
            Double shipping, String city, String state, String country, String currency, List<Map<String,String>> items, String context)
            throws JSONException, IOException, URISyntaxException{
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, order_id);
        //Track ecommerce event.
        if (context != null && !context.equals("")) {
            JSONObject jsonContext = stringToJSON(context);
            this.payload = this.payload.track_ecommerce_transaction_config(order_id, doubleCheck(total_value), stringCheck(affiliation),
                    doubleCheck(tax_value), doubleCheck(shipping), stringCheck(city), stringCheck(state), stringCheck(country),
                    stringCheck(currency), jsonContext);
        } else {
            this.payload = this.payload.track_ecommerce_transaction_config(order_id, doubleCheck(total_value), stringCheck(affiliation),
                    doubleCheck(tax_value), doubleCheck(shipping), stringCheck(city), stringCheck(state), stringCheck(country),
                    stringCheck(currency), null);
        }
        this.track();
        for (Map<String,String> item : items){
            this.track_ecommerce_transaction_item(order_id, mapCheck(item, "sku"), dParseCatch(mapCheck(item, "price")),
                    iParseCatch(mapCheck(item, "quantity")), mapCheck(item, "name"), mapCheck(item, "category"), mapCheck(item, "currency"), null,
                    this.payload.getParam("tid"));
        }
    }

    /* Checker or Helper functions
     *   doubleCheck is used to catch fields that aren't required but double cant be null
     *    and it gets messier handling it with Integers that can be null
     */
    private String integerCheck(Integer i) { return i==null ? "" : String.valueOf(i); }
    private String doubleCheck(Double d) { return d==null ? "" : String.valueOf(d); }
    private String stringCheck(String s) { return s==null ? "" : s; }
    private String mapCheck(Map<String,String> m, String s){ return m.containsKey(s) ? m.get(s) : ""; }
    private double dParseCatch(String s){
        try{ return Double.parseDouble(s); }
        catch (NumberFormatException nfe) { throw new NumberFormatException("Item requires fields: 'sku', 'price','quantity'"); }
    }
    private int iParseCatch(String s){
        try{ return Integer.parseInt(s); }
        catch (NumberFormatException nfe) { throw new NumberFormatException("Item requires fields: 'sku', 'price','quantity'"); }
    }

    /* Web functions
     *   Functions used to configure the Get request
     *   Split into several functions to configure HTTP errors catches in the future
     *   buildURI sets all the parameters up for the HttpGet
     *   HttpGet makes an HttpGet object.
     */
    private URI buildURI(String scheme, String host, String path) throws URISyntaxException{
        URIBuilder uri = new URIBuilder()
                .setScheme(scheme)
                .setHost(host)
                .setPath(path);
        Set<String> params = this.payload.getParamKeySet();
        for (String i : params){
            uri.setParameter(i, this.payload.getParam(i));
        }
        return uri.build();
    }

    // Make a HttpGet class based on the URI
    private HttpGet makeHttpGet(URI uri){
        return new HttpGet(uri);
    }

    // Make the request, do the work you need to, then close the response.
    // All acceptable status codes are in the 200s
    private void makeRequest(HttpGet httpGet)
        throws IOException{
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(httpGet);
        int statusCode = response.getStatusLine().getStatusCode();
        if (Math.floor(statusCode/100)!=2){
            throw new Error("HTTP Error - Error code " + statusCode);
        }
        try{
            if (debug) {
                Header[] headers = response.getAllHeaders();
                for (Header h : headers)
                    System.out.println(h.toString());
            }
        }
        finally {
            response.close();
        }
    }

    //Turn String input into JSONObject
    private JSONObject stringToJSON(String jsonStr) throws JSONException{
        return new JSONObject(jsonStr);
    }
    private JSONObject mapToJSON(Map<String, Object> oMap) { return new JSONObject(oMap); }

    //Clear Payload for next iterations
    private void clearPayload() {
        String[] standards = new String[] {"uid", "res", "vp", "cd", "tz", "lang"};
        PayloadMap standPairs = new PayloadMapC();
        Map<String, String> currentParam = this.payload.getParams();
        for (String s : standards)
            if (currentParam.containsKey(s))
                standPairs.add(s, currentParam.get(s));
        this.setPayload(standPairs);
    }

    /**
     * {@inheritDoc}
     * @param integerContractor A contractor of type integer.
     * @param stringContractor A contractory of type String
     * @param dictionaryContractor A Contractor of type Map with key value of String, Object
     */
    public void setContractors(PlowContractor<Integer> integerContractor, PlowContractor<String> stringContractor,
                               PlowContractor<Map<String,Object>> dictionaryContractor){
        this.integerContractor=integerContractor;
        this.stringContractor=stringContractor;
        this.dictionaryContractor=dictionaryContractor;
    }

    private void setPayload(PayloadMap payload){
        this.payload=payload;
        this.payload = this.payload.add_config("encode_base64", this.base64_encode);
        setStandardNV();
    }

    //Only called once when the Payload class is attacked to the com.snowplow.javaplow.Tracker
    private void setStandardNV(){
        this.payload = this.payload.add_standard_nv_pairs(DEFAULT_PLATFORM, VERSION, this.namespace, this.app_id);
    }

    /**
     * {@inheritDoc}
     * @param param Parameter to be set.
     * @param val Value for the parameter.
     */
    public void setParam(String param, String val){
        this.payload = this.payload.add(param, val);
    }

    /**
     * {@inheritDoc}
     * @param platform The platform being tracked, currently supports "pc", "tv", "mob", "cnsl", and "iot".
     */
    public void setPlatform(String platform){//contract true
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.is_supported_platform, platform);
        this.payload = this.payload.add("p", platform);
    }

    /**
     * {@inheritDoc}
     * @param userID The User ID String.
     */
    public void setUserID(String userID){
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, userID);
        this.payload = this.payload.add("uid", userID);
    }

    /**
     * {@inheritDoc}
     * @param width Width of the screen in pixels.
     * @param height Height of the screen in pixels.
     */
    public void setScreenResolution(int width, int height){
        assert this.integerContractor.checkContract(this.contracts, PlowContractor.positive_number, height);
        assert this.integerContractor.checkContract(this.contracts, PlowContractor.positive_number, width);
        this.payload = this.payload.add("res", String.valueOf(width) + "x" + String.valueOf(height));
    }

    /**
     * {@inheritDoc}
     * @param width Width of the viewport in pixels.
     * @param height Height of the viewport in pixels.
     */
    public void setViewport(int width, int height){
        assert this.integerContractor.checkContract(this.contracts, PlowContractor.positive_number, height);
        assert this.integerContractor.checkContract(this.contracts, PlowContractor.positive_number, width);
        this.payload = this.payload.add("vp", String.valueOf(width) + "x" + String.valueOf(height));
    }

    /**
     * {@inheritDoc}
     * @param depth Depth of the color.
     */
    public void setColorDepth(int depth){
        assert this.integerContractor.checkContract(this.contracts, PlowContractor.positive_number, depth) || depth==0;
        this.payload = this.payload.add("cd", String.valueOf(depth));
    }

    /**
     * {@inheritDoc}
     * @param timezone Timezone where tracking takes place.
     */
    public void setTimezone(String timezone){
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, timezone);
        this.payload = this.payload.add("tz", timezone);
    }

    /**
     * {@inheritDoc}
     * @param language Language for info tracked.
     */
    public void setLanguage(String language){
        assert this.stringContractor.checkContract(this.contracts, PlowContractor.non_empty_string, language);
        this.payload = this.payload.add("lang", language);
    }

    /**
     * {@inheritDoc}
     * @return
     */
    public PayloadMap getPayload(){ return this.payload; }

    //Test case main function
    public static void main(String[] args) throws URISyntaxException, IOException, ClientProtocolException, JSONException {
        ///// GENERICS
        Tracker t1 = new TrackerC("d3rkrsqld9gmqf.cloudfront.net", "com.snowplow.javaplow.Tracker Test", "JavaPlow", "com.saggezza", true, true);
//        t1.track();
        t1.setUserID("User1");
        t1.setLanguage("ital");
        t1.setPlatform("mob");
        t1.setScreenResolution(760, 610);
        String context = "{'Zone':'USA', 'Phone':'Droid', 'Time':'2pm'}";

        ///// E COMMERCE TEST
        Map<String,String> items = new HashMap<String, String>();
        items.put("sku", "SKUVAL"); items.put("quantity","2"); items.put("price","19.99");
        List<Map<String,String>> lst = new LinkedList<Map<String, String>>();
        lst.add(items);
        TrackerC.debug=true;

        /////TRACK TEST
        for (int i = 0; i < 2; i++) {
            t1.track();
            try { Thread.sleep(2000); }
            catch (InterruptedException e){}
            System.out.println("Loop " + i);
            Map<String, Object> dict = new LinkedHashMap<String, Object>();
            dict.put("Iteration", i);
            t1.track_unstruct_event("Lube Insights", "Data Loop", dict, context);
            t1.track_struct_event("Items", "Stuff", "Pants", "Green Blue", 3, DEFAULT_VENDOR, context);
            t1.track_page_view("www.saggezza.com", "Saggezza Home", "KG", null);
            t1.track_ecommerce_transaction_item("IT1023", "SKUVAL", 29.99, 2, "boots", "Shoes","USD",null,null);
            t1.track_ecommerce_transaction("OID", 19.99, "Kohls", 2.50, 1.99, "Chagrin", "OH", "USA", "USD", lst, context);
        }
    }
}
