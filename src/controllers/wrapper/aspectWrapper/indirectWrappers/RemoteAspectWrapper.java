package controllers.wrapper.aspectWrapper.indirectWrappers;

import controllers.schema.SchemaReader;
import controllers.wrapper.aspectWrapper.GeneralAspectWrapper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import util.WebGetter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

public class RemoteAspectWrapper extends GeneralAspectWrapper {

    private String url;

    public RemoteAspectWrapper() {
        super();
        url = null;
    }

    public RemoteAspectWrapper(String name, String url) {
        super(name);
        this.url = url;
        if (url != null) {
            try {
                URL urlObject = new URL(url + "/schema");
                URLConnection connection = urlObject.openConnection();
                connection.connect();
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                SchemaReader sReader = new SchemaReader(in);
                this.schema = sReader.getSchemaObject();
                this.isValid = true;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                this.schema = null;
                this.isValid = false;
            }
        }
    }

    @Override
    public JSONArray getResultAsJSONArray(JSONObject searchConditions) {
        Map<String, String> params = new HashMap<String, String>();
        for (Object k : searchConditions.keySet()) {
            String key = (String)k;
            params.put(key, searchConditions.getString(key));
        }
        return JSONArray.fromObject(WebGetter.getResponseString(url, params));
    }

    @Override
    public JSONArray getRegisteredSources() {
        return new JSONArray();
    }

    @Override
    public JSONArray timedGetResultAsJSONArray(JSONObject searchConditions)  {
        System.out.println("Retrieving data pertaining to aspect " + name + ".");
        long startTime = System.currentTimeMillis();
        JSONArray ret = getResultAsJSONArray(searchConditions);
        long endTime = System.currentTimeMillis();
        System.out.println("Aspect " + name + " finished execution. Time elapsed: " + ((double)(endTime - startTime)/1000.0) + " seconds.");
        return ret;
    }

    @Override
    public void print() {
        System.out.println(name + " at " + url + "\n");
    }
}
