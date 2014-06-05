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

import org.json.*;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Set;

//Immutable structure -- Payload will always be a string.

public interface PayloadMap {
    //Add functions
    public PayloadMap add(String key, String val);
    public PayloadMap add_unstruct(JSONObject dictInfo, boolean encode_base64)
            throws UnsupportedEncodingException;
    public PayloadMap add_json(JSONObject jsonObject, boolean encode_base64)
            throws UnsupportedEncodingException;
    public PayloadMap add_standard_nv_pairs(String p, String tv, String tna, String aid);
    public PayloadMap add_config(String config_title, boolean config);

    //Track configs
    public PayloadMap track_page_view_config(String page_url, String page_title, String referrer,
                                             JSONObject context) throws UnsupportedEncodingException;
    public PayloadMap track_struct_event_config(String category, String action, String label, String property,
                                                String value, JSONObject context)throws UnsupportedEncodingException;
    public PayloadMap track_unstruct_event_config(String eventVendor, String eventName, JSONObject dictInfo,
                                                  JSONObject context) throws UnsupportedEncodingException;
    public PayloadMap track_ecommerce_transaction_item_config(String order_id, String sku, String price, String quantity,
                                                              String name, String category, String currency, JSONObject context, String transaction_id)
            throws UnsupportedEncodingException;
    public PayloadMap track_ecommerce_transaction_config(String order_id, String total_value, String affiliation, String tax_value,
                                                         String shipping, String city, String state, String country, String currency, JSONObject context)
            throws UnsupportedEncodingException;
    //Setters
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
