import controllers.wrapper.sourceWrapper.interfaces.TupleExtractor;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.lang.Override;

public class MyTupleExtractor implements TupleExtractor{

    @Override
    public JSONArray getTuples(JSON all) {
        JSONObject allAsObject = JSONObject.fromObject(all);
        return allAsObject.getJSONArray("document");
    }

}