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

package javaplow;

import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * PayloadMap interface
 *  The PayloadMap is used to store all the parameters and configurations that are used
 *  to send data via the HTTP GET request.
 * PayloadMaps have an immutable structure for secure and accurate transfers of information.
 * @version 0.0.2
 * @author Kevin Gleason
 */

//Immutable structure -- Payload will always be a string.

public interface PayloadMap {
    /**
     * Add a basic parameter.
     * @param key The parameter key
     * @param val The parameter value
     * @return Returns a new PayloadMap with the key-value-pair
     */
    public PayloadMap add(String key, String val);

    /**
     * Add an unstructured source
     * @param dictInfo Information is parsed elsewhere from string to JSON then passed here
     * @param encode_base64 Whether or not to encode before transferring to web. Default true.
     * @return Returns a new PayloadMap with the key-value-pair
     * @throws UnsupportedEncodingException
     */
    public PayloadMap add_unstruct(JSONObject dictInfo, boolean encode_base64)
            throws UnsupportedEncodingException;

    /**
     * Add a JSON Object to the Payload
     * @param jsonObject JSON object to be added
     * @param encode_base64 Whether or not to encode before transferring to web. Default true.
     * @return Returns a new PayloadMap with the key-value-pair
     * @throws UnsupportedEncodingException
     */
    public PayloadMap add_json(JSONObject jsonObject, boolean encode_base64)
            throws UnsupportedEncodingException;

    /**
     * Add the standard name-value-pairs, snowplow depends on them.
     * @see javaplow.TrackerC
     * @param p Platform
     * @param tv Tracker Version
     * @param tna Namespace
     * @param aid App_id
     * @return Returns a new PayloadMap with the key-value-pair
     */
    public PayloadMap add_standard_nv_pairs(String p, String tv, String tna, String aid);

    /**
     * Add a configuration to the payload. Used currently for "base64_encode"
     * @param config_title Key of the configuration
     * @param config Value of the configuration
     * @return Returns a new PayloadMap with the key-value-pair
     */
    public PayloadMap add_config(String config_title, boolean config);

    /**
     * Configuration of the PayloadMap for a track page view call.
     * @param page_url The URL or the page being tracked.
     * @param page_title The Title of the page being tracked.
     * @param referrer The referrer of the page being tracked.
     * @param context Additional JSON context (optional)
     * @return Returns a new PayloadMap with the key-value-pair
     * @throws UnsupportedEncodingException
     */
    public PayloadMap track_page_view_config(String page_url, String page_title, String referrer,
                                             JSONObject context) throws UnsupportedEncodingException;

    /**
     * Configuration for tracking a structured event
     * @param category Category of the event being tracked.
     * @param action Action of the event being tracked
     * @param label Label of the event being tracked.
     * @param property Property of the event being tracked.
     * @param value Value associated with the property being tracked.
     * @param context Additional JSON context (optional)
     * @return Returns a new PayloadMap with the key-value-pairs
     * @throws UnsupportedEncodingException
     */
    public PayloadMap track_struct_event_config(String category, String action, String label, String property,
                                                String value, JSONObject context)throws UnsupportedEncodingException;

    /**
     * Configuration to track an unstructured event.
     * @param eventVendor The vendor the the event information.
     * @param eventName A name for the unstructured event being tracked.
     * @param dictInfo The unstructured information being tracked in dictionary form.
     * @param context Additional JSON context for the tracking call (optional)
     * @throws UnsupportedEncodingException If JSON is in improper formatting
     */
    public PayloadMap track_unstruct_event_config(String eventVendor, String eventName, JSONObject dictInfo,
                                                  JSONObject context) throws UnsupportedEncodingException;
    /**
     * Configuration to track an ecommerce transaction item. Not usually called alone, but called for each
     * individual item of the ecommerce transaction function.
     * @param order_id ID of the item.
     * @param sku SKU value of the item.
     * @param price Prive of the item.
     * @param quantity Quantity of the item.
     * @param name Name of the item.
     * @param category Category of the item.
     * @param currency Currency used for the purchase.
     * @param context Additional JSON context for the tracking call (optional)
     * @param transaction_id Transaction ID, if left blank new value will be generated.
     * @throws java.io.UnsupportedEncodingException
     * @return Returns a new PayloadMap with the key-value-pairs
     */
    public PayloadMap track_ecommerce_transaction_item_config(String order_id, String sku, String price, String quantity,
            String name, String category, String currency, JSONObject context, String transaction_id)
            throws UnsupportedEncodingException;

    /**
     * Track an Ecommerce Transaction
     * Option to provide a Map of only strings of items in the transaction which can be used
     * to track each individual ecommerce transaction item
     * @param order_id The transaction ID, will be generated if left null
     * @param total_value The total value of the transaction.
     * @param affiliation Affiliations to the transaction (optional)
     * @param tax_value Tax value of the transaction (optional)
     * @param shipping Shipping costs of the transaction (optional)
     * @param city The customers city.
     * @param state The customers state.
     * @param country The customers country.
     * @param currency The currency used for the purchase
     * @param context Additional JSON context for the tracking call (optional)
     * @return Returns a new PayloadMap with the key-value-pair
     * @throws UnsupportedEncodingException
     */
    public PayloadMap track_ecommerce_transaction_config(String order_id, String total_value, String affiliation, String tax_value,
                                                         String shipping, String city, String state, String country, String currency, JSONObject context)
            throws UnsupportedEncodingException;

    /**
     * Sets the timestamp on a PayloadMap
     * @return Returns a new PayloadMap with the key-value-pair
     */
    public PayloadMap setTimestamp();

    //Getters
    public Set getParamKeySet();
    public Set getConfigKeySet();
    public LinkedHashMap<String,String> getParams();
    public LinkedHashMap<String,Boolean> getConfigs();
    public String getParam(String key);
    public boolean getConfig(String key);
    public String toString();
}
