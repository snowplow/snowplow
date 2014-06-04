// File: PayloadMap.java
// Author: Kevin Gleason
// Date: 5/28/14
// Use: The interface for PayloadMap,
  // used to create the get request payload

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.json.*;

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
    public PayloadMap track_page_view_config(String page_url, String page_title, String referrer,
            String vendor, JSONObject context) throws UnsupportedEncodingException;
    public PayloadMap track_struct_event_config(String category, String action, String label, String property,
            String value, String vendor, JSONObject context)throws UnsupportedEncodingException;
    public PayloadMap track_unstruct_event_config(String eventVendor, String eventName, JSONObject dictInfo,
           JSONObject context) throws UnsupportedEncodingException;
    public PayloadMap track_ecommerce_transaction_item_config(String order_id, String sku, double price, int quantity,
            String name, String category, String currency, String vendor, JSONObject context)
            throws UnsupportedEncodingException;

    //Getters
    public Set getParamKeySet();
    public Set getConfigKeySet();
    public LinkedHashMap<String,String> getParams();
    public LinkedHashMap<String,Boolean> getConfigs();
    public String getParam(String key);
    public boolean getConfig(String key);
    public String toString();
}
