package controllers.wrapper.sourceWrapper.interfaces;

import net.sf.json.JSON;

/**
 * Standard interface for a formatter
 */
public interface Formatter {

    /*
    * Input: raw response generated by getter
    * Output: JSON object representing the data
    * */
    public JSON convertToJSON(Object rawResponse);

}
