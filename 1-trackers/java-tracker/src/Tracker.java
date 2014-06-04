// File: Tracker.java
// Author: Kevin Gleason
// Date: 5/28/14
// Use: The tracker interface for TrackerC

import org.apache.http.client.ClientProtocolException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Map;

public interface Tracker {

    //Tracking Events
    public void track() throws URISyntaxException, ClientProtocolException, IOException;
    public void track_page_view(String page_url, String page_title, String referrer, String context)
            throws UnsupportedEncodingException, IOException, URISyntaxException, JSONException;
    public void track_struct_event(String category, String action, String label, String property,
            int value, String vendor, String context)throws JSONException, URISyntaxException, IOException;
    public void track_unstruct_event(String eventVendor, String eventName, String dictInfo, String context)
            throws JSONException, IOException, URISyntaxException;
    public void track_screen_view(String name, String id, String context)
            throws JSONException, IOException, URISyntaxException;
    public void track_ecommerce_transaction_item(String order_id, String sku, double price, int quantity, String name,
            String category, String currency, String context)throws JSONException, URISyntaxException, IOException;
    //Setters
    public void setContractors(PlowContractor<Integer> integerContractor, PlowContractor<String> stringContractor,
                               PlowContractor<Map<String,Object>> dictionaryContractor);
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
