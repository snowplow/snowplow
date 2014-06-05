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

import org.json.JSONException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public interface Tracker {

    //Tracking Events
    public void track() throws URISyntaxException, IOException;
    public void track_page_view(String page_url, String page_title, String referrer, String context)
            throws IOException, URISyntaxException, JSONException;
    public void track_struct_event(String category, String action, String label, String property,
            int value, String vendor, String context) throws JSONException, URISyntaxException, IOException;
    public void track_unstruct_event(String eventVendor, String eventName, String dictInfo, String context)
            throws JSONException, IOException, URISyntaxException;
    public void track_screen_view(String name, String id, String context)
            throws JSONException, IOException, URISyntaxException;
    public void track_ecommerce_transaction_item(String order_id, String sku, Double price, Integer quantity, String name,
            String category, String currency, String context, String transaction_id)throws JSONException, URISyntaxException, IOException;
    public void track_ecommerce_transaction(String order_id, Double total_value, String affiliation, Double tax_value,
            Double shipping, String city, String state, String country, String currency, List<Map<String, String>> items, String context)
            throws JSONException, IOException, URISyntaxException;

    //Setters
    public void setContractors(PlowContractor<Integer> integerContractor, PlowContractor<String> stringContractor,
            PlowContractor<Map<String, Object>> dictionaryContractor);
    public void setPayload(PayloadMap payload);
    public void setParam(String param, String val);
    public void setPlatform(String platform);
    public void setUserID(String userID);
    public void setScreenResolution(int width, int height);
    public void setViewport(int width, int height);
    public void setColorDepth(int depth);
    public void setTimezone(String timezone);
    public void setLanguage(String language);
    public PayloadMap getPayload();
}
